package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.ActorContext
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ConsoleMessageHandler.ConsoleMessageTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkCompletedHandler.LinkCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionStartedHandler.WorkerStateUpdated
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage
import edu.uci.ics.amber.engine.architecture.logging.{
  LogManager,
  ProcessControlMessage,
  SenderActorChange
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager
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
import edu.uci.ics.amber.engine.common.virtualidentity.util.{
  CONTROLLER,
  SELF,
  SOURCE_STARTER_ACTOR,
  SOURCE_STARTER_OP
}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.amber.engine.common.{
  AmberLogging,
  IOperatorExecutor,
  ISourceOperatorExecutor,
  InputExhausted
}
import edu.uci.ics.amber.error.ErrorUtils.{mkConsoleMessage, safely}

import java.util.concurrent.{ExecutorService, Executors, Future}

class DataProcessor( // dependencies:
    val workerIndex: Int,
    var operator: IOperatorExecutor, // core logic
    asyncRPCClient: AsyncRPCClient, // to send controls
    outputManager: OutputManager, // to send output tuples
    breakpointManager: BreakpointManager, // to evaluate breakpoints
    stateManager: WorkerStateManager,
    val upstreamLinkStatus: UpstreamLinkStatus,
    asyncRPCServer: AsyncRPCServer,
    val logStorage: DeterminantLogStorage,
    val logManager: LogManager,
    val recoveryManager: LocalRecoveryManager,
    val recoveryQueue: RecoveryQueue,
    val actorId: ActorVirtualIdentity,
    val actorContext: ActorContext, // context of this actor
    var opExecConfig: OpExecConfig
) extends AmberLogging {

  val pauseManager: PauseManager = new PauseManager(this)
  val epochManager: EpochManager = new EpochManager(this, outputManager, asyncRPCServer)
  val internalQueue = new WorkerInternalQueue(pauseManager, logManager, recoveryQueue)

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
                internalQueue.restoreInputs()
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
                FatalError(
                  new WorkflowRuntimeException("DP Thread exists unexpectedly", err),
                  Some(actorId)
                ),
                CONTROLLER
              )
          }
        }
      })
    }
  }

  if (this.operator.isInstanceOf[ISourceOperatorExecutor]) {
    // for source operator: add a virtual input channel just for kicking off the execution
    registerInput(SOURCE_STARTER_ACTOR, LinkIdentity(SOURCE_STARTER_OP, 0, this.opExecConfig.id, 0))
  }

  def registerInput(identifier: ActorVirtualIdentity, input: LinkIdentity): Unit = {
    upstreamLinkStatus.registerInput(identifier, input)
    internalQueue.registerInput(identifier)
  }

  def getInputPort(identifier: ActorVirtualIdentity): Int = {
    val inputLink = upstreamLinkStatus.getInputLink(identifier)
    if (inputLink.from == SOURCE_STARTER_OP) 0 // special case for source operator
    else if (!opExecConfig.inputToOrdinalMapping.contains(inputLink)) 0
    else opExecConfig.inputToOrdinalMapping(inputLink)
  }

  def getOutputLinkByPort(outputPort: Option[Int]): List[LinkIdentity] = {
    if (outputPort.isEmpty) {
      opExecConfig.outputToOrdinalMapping.keySet.toList
    } else {
      opExecConfig.outputToOrdinalMapping.filter(p => p._2 == outputPort.get).keys.toList
    }
  }

  // dp thread stats:
  // TODO: add another variable for recovery index instead of using the counts below.
  private var inputTupleCount = 0L
  private var outputTupleCount = 0L
  private var currentInputTuple: Either[ITuple, InputExhausted] = _
  private var currentInputActor: ActorVirtualIdentity = _
  private var currentOutputIterator: Iterator[(ITuple, Option[Int])] = _

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
  private[this] def processInputTuple(): Iterator[(ITuple, Option[Int])] = {
    var outputIterator: Iterator[(ITuple, Option[Int])] = null
    try {
      outputIterator = operator.processTuple(
        currentInputTuple,
        getInputPort(currentInputActor),
        pauseManager,
        asyncRPCClient
      )
      if (currentInputTuple.isLeft) {
        inputTupleCount += 1
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
    var out: (ITuple, Option[Int]) = null
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
      pauseManager.pause(UserPause)
      outputManager.adaptiveBatchingMonitor.pauseAdaptiveBatching()
      stateManager.transitTo(PAUSED)
    } else {
      outputTupleCount += 1
      val outLinks = getOutputLinkByPort(outputPortOpt)
      outLinks.foreach(link => outputManager.passTupleToDownstream(outputTuple, link))
    }
  }

  private[this] def internalQueueElementHandler(
      internalQueueElement: InternalQueueElement
  ): Unit = {
    logManager.getDeterminantLogger.stepIncrement()
    internalQueueElement match {
      case InputTuple(from, tuple) =>
        if (stateManager.getCurrentState == READY) {
          stateManager.transitTo(RUNNING)
          outputManager.adaptiveBatchingMonitor.enableAdaptiveBatching(actorContext)
          asyncRPCClient.send(
            WorkerStateUpdated(stateManager.getCurrentState),
            CONTROLLER
          )
        }
        if (currentInputActor != from) {
          logManager.getDeterminantLogger.logDeterminant(SenderActorChange(from))
          currentInputActor = from
        }
        currentInputTuple = Left(tuple)
        handleInputTuple()
      case EndMarker(from) =>
        if (currentInputActor != from) {
          logManager.getDeterminantLogger.logDeterminant(SenderActorChange(from))
          currentInputActor = from
        }
        processControlCommandsDuringExecution() // necessary for trigger correct recovery
        upstreamLinkStatus.markWorkerEOF(from)
        val currentLink = upstreamLinkStatus.getInputLink(currentInputActor)
        if (upstreamLinkStatus.isLinkEOF(currentLink)) {
          currentInputTuple = Right(InputExhausted())
          handleInputTuple()
          if (currentLink != null) {
            asyncRPCClient.send(LinkCompleted(currentLink), CONTROLLER)
          }
        }
        if (upstreamLinkStatus.isAllEOF) {
          outputManager.emitEndOfUpstream()
          // Send Completed signal to worker actor.
          logger.info(s"$operator completed")
          operator.close() // close operator
          asyncRPCClient.send(WorkerExecutionCompleted(), CONTROLLER)
          outputManager.adaptiveBatchingMonitor.pauseAdaptiveBatching()
          stateManager.transitTo(COMPLETED)
        }
      case ControlElement(from, payload) =>
        processControlCommand(from, payload)
      case InputEpochMarker(from, epochMarker) =>
        epochManager.processEpochMarker(from, epochMarker)
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
      internalQueueElementHandler(internalQueue.getElement)
    }
  }

  private[this] def handleOperatorException(e: Throwable): Unit = {
    asyncRPCClient.send(
      ConsoleMessageTriggered(mkConsoleMessage(actorId, e)),
      CONTROLLER
    )
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
      outputIterator: Iterator[(ITuple, Option[Int])]
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
    while (internalQueue.isControlQueueNonEmptyOrPaused) {
      takeOneControlCommandAndProcess()
    }
  }

  private[this] def takeOneControlCommandAndProcess(): Unit = {
    val control = internalQueue.getElement.asInstanceOf[ControlElement]
    processControlCommand(control.payload, control.from)
  }

  private[this] def processControlCommand(
      payload: ControlPayload,
      from: ActorVirtualIdentity
  ): Unit = {
    logManager.getDeterminantLogger.logDeterminant(ProcessControlMessage(payload, from))
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
