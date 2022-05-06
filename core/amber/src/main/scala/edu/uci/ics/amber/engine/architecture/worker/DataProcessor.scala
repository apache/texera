package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkCompletedHandler.LinkCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalOperatorExceptionHandler.LocalOperatorException
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.architecture.messaginglayer.TupleToBatchConverter
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue._
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.COMPLETED
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{COMPLETED, PAUSED}
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
    pauseManager: PauseManager, // to pause/resume
    breakpointManager: BreakpointManager, // to evaluate breakpoints
    stateManager: WorkerStateManager,
    asyncRPCServer: AsyncRPCServer,
    val actorId: ActorVirtualIdentity
) extends WorkerInternalQueue
    with AmberLogging {
  // initialize dp thread upon construction
  private val dpThreadExecutor: ExecutorService = Executors.newSingleThreadExecutor
  private val dpThread: Future[_] = dpThreadExecutor.submit(new Runnable() {
    def run(): Unit = {
      try {
        runDPThreadMainLogic()
      } catch safely {
        case _: InterruptedException =>
          logger.info("DP Thread exits")
        case err: Exception =>
          logger.error("DP Thread exists unexpectedly", err)
          asyncRPCClient.send(
            FatalError(new WorkflowRuntimeException("DP Thread exists unexpectedly", err)),
            CONTROLLER
          )
        // dp thread will stop here
      }
    }
  })
  // dp thread stats:
  // TODO: add another variable for recovery index instead of using the counts below.
  private var inputTupleCount = 0L
  private var outputTupleCount = 0L
  private var currentInputTuple: Either[ITuple, InputExhausted] = _
  private var currentInputLink: LinkIdentity = _
  private var currentOutputIterator: Iterator[(ITuple, Option[LinkIdentity])] = _
  private var isCompleted = false

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
      outputIterator = operator.processTuple(currentInputTuple, currentInputLink)
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
      pauseManager.pause()
      disableDataQueue()
      stateManager.transitTo(PAUSED)
    } else {
      outputTupleCount += 1
      batchProducer.passTupleToDownstream(outputTuple, outputPortOpt)
    }
  }

  /** Provide main functionality of data processing
    *
    * @throws Exception (from engine code only)
    */
  @throws[Exception]
  private[this] def runDPThreadMainLogic(): Unit = {
    // main DP loop
    while (!isCompleted) {
      // take the next data element from internal queue, blocks if not available.
      getElement match {
        case InputTuple(tuple) =>
          currentInputTuple = Left(tuple)
          handleInputTuple()
        case SenderChangeMarker(link) =>
          currentInputLink = link
        case EndMarker =>
          currentInputTuple = Right(InputExhausted())
          handleInputTuple()
          if (currentInputLink != null) {
            asyncRPCClient.send(LinkCompleted(currentInputLink), CONTROLLER)
          }
        case EndOfAllMarker =>
          // end of processing, break DP loop
          isCompleted = true
          batchProducer.emitEndOfUpstream()
        case ControlElement(payload, from) =>
          processControlCommand(payload, from)
      }
    }
    // Send Completed signal to worker actor.
    logger.info(s"$operator completed")
    disableDataQueue()
    operator.close() // close operator
    asyncRPCClient.send(WorkerExecutionCompleted(), CONTROLLER)
    stateManager.transitTo(COMPLETED)
    processControlCommandsAfterCompletion()
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
    while (!isControlQueueEmpty || pauseManager.isPaused) {
      takeOneControlCommandAndProcess()
    }
  }

  private[this] def processControlCommandsAfterCompletion(): Unit = {
    while (true) {
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
