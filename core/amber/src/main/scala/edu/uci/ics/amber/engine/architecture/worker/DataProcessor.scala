package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkCompletedHandler.LinkCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalOperatorExceptionHandler.LocalOperatorException
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionStartedHandler.WorkerStateUpdated
import edu.uci.ics.amber.engine.architecture.logging.service.TimeService
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogWriter
import edu.uci.ics.amber.engine.architecture.logging.{
  LogManager,
  ProcessControlMessage,
  SenderActorChange
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.TupleToBatchConverter
import edu.uci.ics.amber.engine.architecture.recovery.{LocalRecoveryManager, RecoveryQueue}
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue._
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{
  COMPLETED,
  PAUSED,
  READY,
  RUNNING,
  UNINITIALIZED
}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayload
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCServer}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.amber.engine.common.{AmberLogging, IOperatorExecutor, InputExhausted}
import edu.uci.ics.amber.error.ErrorUtils.safely

import java.util.concurrent.{ExecutorService, Executors, Future}
import scala.collection.mutable

class DataProcessor( // dependencies:
    operator: IOperatorExecutor, // core logic
    asyncRPCClient: AsyncRPCClient, // to send controls
    batchProducer: TupleToBatchConverter, // to send output tuples
    val pauseManager: PauseManager, // to pause/resume
    breakpointManager: BreakpointManager, // to evaluate breakpoints
    stateManager: WorkerStateManager,
    upstreamLinkStatus: UpstreamLinkStatus,
    asyncRPCServer: AsyncRPCServer,
    val logStorage: DeterminantLogStorage,
    val logManager: LogManager,
    val recoveryManager: LocalRecoveryManager,
    val recoveryQueue: RecoveryQueue,
    val actorId: ActorVirtualIdentity
) extends WorkerInternalQueue
    with AmberLogging {
  // initialize dp thread upon construction
  private val dpThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor
  private var dpThread: Future[_] = _
  def start(): Unit = {
    if (dpThread == null) {
      dpThread = dpThreadExecutor.submit(new Runnable() {
        def run(): Unit = {
          try {
            // TODO: setup context
            stateManager.assertState(UNINITIALIZED)
            // operator.context = new OperatorContext(new TimeService(logManager))
            stateManager.transitTo(READY)
            if (!recoveryQueue.isReplayCompleted) {
              recoveryQueue.registerOnEnd(() => recoveryManager.End())
              recoveryManager.Start()
              recoveryManager.registerOnEnd(() => {
                logger.info("recovery complete! restoring stashed inputs...")
                logManager.terminate()
                logStorage.cleanPartiallyWrittenLogFile()
                logManager.setupWriter(logStorage.getWriter)
                restoreInputs()
                logger.info("stashed inputs restored!")
              })
            }
            runDPThreadMainLogic()
          } catch safely {
            case _: InterruptedException =>
              // dp thread will stop here
              logger.info("DP Thread exits")
            case err: Exception =>
              logger.error("DP Thread exists unexpectedly", err)
              asyncRPCClient.send(
                FatalError(new WorkflowRuntimeException("DP Thread exists unexpectedly", err)),
                CONTROLLER
              )
          }
        }
      })
    }
  }

  /**
    * Map from Identifier to input number. Used to convert the Identifier
    * to int when adding sender info to the queue.
    * We also keep track of the upstream actors so that we can emit
    * EndOfAllMarker when all upstream actors complete their job
    */
  private val inputMap = new mutable.HashMap[ActorVirtualIdentity, LinkIdentity]

  def registerInput(identifier: ActorVirtualIdentity, input: LinkIdentity): Unit = {
    inputMap(identifier) = input
  }

  def getInputLink(identifier: ActorVirtualIdentity): LinkIdentity = {
    if (identifier != null) {
      inputMap(identifier)
    } else {
      null // special case for source operator
    }
  }
  // dp thread stats:
  // TODO: add another variable for recovery index instead of using the counts below.
  private var inputTupleCount = 0L
  private var outputTupleCount = 0L
  private var currentInputTuple: Either[ITuple, InputExhausted] = _
  private var currentInputActor: ActorVirtualIdentity = _
  private var currentOutputIterator: Iterator[(ITuple, Option[LinkIdentity])] = _

  def getOperatorExecutor(): IOperatorExecutor = operator

  /** provide API for actor to get stats of this operator
    *
    * @return (input tuple count, output tuple count)
    */
  def collectStatistics(): (Long, Long) = (inputTupleCount, outputTupleCount)

  /** provide API for actor to get current input tuple of this operator
    *
    * @return current input tuple if it exists
    */
  def getCurrentInputTuple: ITuple = {
    if (currentInputTuple != null && currentInputTuple.isLeft) {
      currentInputTuple.left.get
    } else {
      null
    }
  }

  def setCurrentTuple(tuple: Either[ITuple, InputExhausted]): Unit = {
    currentInputTuple = tuple
  }

  def shutdown(): Unit = {
    dpThread.cancel(true) // interrupt
    dpThreadExecutor.shutdownNow() // destroy thread
  }

  /** process currentInputTuple through operator logic.
    * this function is only called by the DP thread
    *
    * @return an iterator of output tuples
    */
  private[this] def processInputTuple(): Iterator[(ITuple, Option[LinkIdentity])] = {
    var outputIterator: Iterator[(ITuple, Option[LinkIdentity])] = null
    try {
      outputIterator = operator.processTuple(
        currentInputTuple,
        getInputLink(currentInputActor),
        pauseManager,
        asyncRPCClient
      )
      if (currentInputTuple.isLeft) {
        inputTupleCount += 1
      }
      if (pauseManager.getPauseStatusByType(PauseType.OperatorLogicPause)) {
        // if the operatorLogic decides to pause, we need to disable the data queue for this worker.
        disableDataQueue()
      }
    } catch safely {
      case e =>
        // forward input tuple to the user and pause DP thread
        handleOperatorException(e)
    }
    outputIterator
  }

  /** transfer one tuple from iterator to downstream.
    * this function is only called by the DP thread
    */
  private[this] def outputOneTuple(): Unit = {
    var out: (ITuple, Option[LinkIdentity]) = null
    try {
      out = currentOutputIterator.next
    } catch safely {
      case e =>
        // invalidate current output tuple
        out = null
        // also invalidate outputIterator
        currentOutputIterator = null
        // forward input tuple to the user and pause DP thread
        handleOperatorException(e)
    }
    if (out == null) return

    val (outputTuple, outputPortOpt) = out
    if (breakpointManager.evaluateTuple(outputTuple)) {
      pauseManager.recordRequest(PauseType.UserPause, true)
      disableDataQueue()
      stateManager.transitTo(PAUSED)
    } else {
      outputTupleCount += 1
      batchProducer.passTupleToDownstream(outputTuple, outputPortOpt)
    }
  }

  private[this] def internalQueueElementHandler(
      internalQueueElement: InternalQueueElement
  ): Unit = {
    internalQueueElement match {
      case InputTuple(from, tuple) =>
        if (stateManager.getCurrentState == READY) {
          stateManager.transitTo(RUNNING)
          asyncRPCClient.send(
            WorkerStateUpdated(stateManager.getCurrentState),
            CONTROLLER
          )
        }
        if (currentInputActor != from) {
          determinantLogger.logDeterminant(SenderActorChange(from))
          currentInputActor = from
        }
        currentInputTuple = Left(tuple)
        handleInputTuple()
      case EndMarker(from) =>
        if (currentInputActor != from) {
          determinantLogger.logDeterminant(SenderActorChange(from))
          currentInputActor = from
        }
        processControlCommandsDuringExecution() // necessary for trigger correct recovery
        upstreamLinkStatus.markWorkerEOF(from)
        val currentLink = getInputLink(currentInputActor)
        if (upstreamLinkStatus.isLinkEOF(currentLink)) {
          currentInputTuple = Right(InputExhausted())
          handleInputTuple()
          asyncRPCClient.send(LinkCompleted(currentLink), CONTROLLER)
        }
        if (upstreamLinkStatus.isAllEOF) {
          batchProducer.emitEndOfUpstream()
          // Send Completed signal to worker actor.
          logger.info(s"$operator completed")
          disableDataQueue()
          operator.close() // close operator
          asyncRPCClient.send(WorkerExecutionCompleted(), CONTROLLER)
          stateManager.transitTo(COMPLETED)
        }
      case ControlElement(payload, from) =>
        processControlCommand(payload, from)
    }
  }

  /** Provide main functionality of data processing
    *
    * @throws Exception (from engine code only)
    */
  @throws[Exception]
  private[this] def runDPThreadMainLogic(): Unit = {
    // main DP loop
    while (true) {
      // take the next data element from internal queue, blocks if not available.
      internalQueueElementHandler(getElement)
    }
  }

  private[this] def handleOperatorException(e: Throwable): Unit = {
    if (currentInputTuple.isLeft) {
      asyncRPCClient.send(
        LocalOperatorException(currentInputTuple.left.get, e),
        CONTROLLER
      )
    } else {
      asyncRPCClient.send(
        LocalOperatorException(ITuple("input exhausted"), e),
        CONTROLLER
      )
    }
    logger.warn(e.getLocalizedMessage + "\n" + e.getStackTrace.mkString("\n"))
    // invoke a pause in-place
    asyncRPCServer.execute(PauseWorker(), SELF)
  }

  private[this] def handleInputTuple(): Unit = {
    // process controls before processing the input tuple.
    processControlCommandsDuringExecution()
    if (currentInputTuple != null) {
      // pass input tuple to operator logic.
      currentOutputIterator = processInputTuple()
      // process controls before outputting tuples.
      processControlCommandsDuringExecution()
      // output loop: take one tuple from iterator at a time.
      while (outputAvailable(currentOutputIterator)) {
        // send tuple to downstream.
        outputOneTuple()
        // process controls after one tuple has been outputted.
        processControlCommandsDuringExecution()
      }
    }
  }

  /**
    * Called by skewed worker in Reshape when it has received the tuples from the helper
    * and is ready to output tuples.
    * The call comes from AcceptMutableStateHandler.
    *
    * @param iterator
    */
  def setCurrentOutputIterator(iterator: Iterator[ITuple]): Unit = {
    currentOutputIterator = iterator.map(t => (t, Option.empty))
  }

  private[this] def outputAvailable(
      outputIterator: Iterator[(ITuple, Option[LinkIdentity])]
  ): Boolean = {
    try {
      outputIterator != null && outputIterator.hasNext
    } catch safely {
      case e =>
        handleOperatorException(e)
        false
    }
  }

  private[this] def processControlCommandsDuringExecution(): Unit = {
    while (isControlQueueNonEmptyOrPaused) {
      takeOneControlCommandAndProcess()
    }
  }

  private[this] def takeOneControlCommandAndProcess(): Unit = {
    val control = getElement.asInstanceOf[ControlElement]
    processControlCommand(control.payload, control.from)
  }

  private[this] def processControlCommand(
      payload: ControlPayload,
      from: ActorVirtualIdentity
  ): Unit = {
    determinantLogger.logDeterminant(ProcessControlMessage(payload, from))
    payload match {
      case invocation: ControlInvocation =>
        asyncRPCServer.logControlInvocation(invocation, from)
        asyncRPCServer.receive(invocation, from)
      case ret: ReturnInvocation =>
        asyncRPCClient.logControlReply(ret, from)
        asyncRPCClient.fulfillPromise(ret)
    }
  }

}
