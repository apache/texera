package edu.uci.ics.amber.engine.architecture.worker

import java.util.concurrent.{Executors, LinkedBlockingDeque}

import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkCompletedHandler.LinkCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalOperatorExceptionHandler.LocalOperatorException
import edu.uci.ics.amber.engine.architecture.messaginglayer.TupleToBatchConverter
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{
  EndMarker,
  EndOfAllMarker,
  InputTuple,
  InternalQueueElement,
  SenderChangeMarker,
  UnblockForControlCommands
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCServer}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.{Completed, Running}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, InputExhausted, WorkflowLogger}
import edu.uci.ics.amber.error.WorkflowRuntimeError
import edu.uci.ics.amber.error.ErrorUtils.safely

class DataProcessor( // dependencies:
    logger: WorkflowLogger, // logger of the worker actor
    operator: IOperatorExecutor, // core logic
    asyncRPCClient: AsyncRPCClient, // to send controls
    batchProducer: TupleToBatchConverter, // to send output tuples
    pauseManager: PauseManager, // to pause/resume
    breakpointManager: BreakpointManager, // to evaluate breakpoints
    stateManager: WorkerStateManager,
    asyncRPCServer: AsyncRPCServer
) extends WorkerInternalQueue {
  // dp thread stats:
  // TODO: add another variable for recovery index instead of using the counts below.
  private var inputTupleCount = 0L
  private var outputTupleCount = 0L
  private var currentInputTuple: Either[ITuple, InputExhausted] = _
  private var currentInputLink: LinkIdentity = _
  private var currentOutputIterator: Iterator[ITuple] = _
  private var isCompleted = false

  // initialize dp thread upon construction
  private val dpThreadExecutor = Executors.newSingleThreadExecutor
  private val dpThread = dpThreadExecutor.submit(new Runnable() {
    def run(): Unit = {
      try {
        // initialize operator
        operator.open()
        runDPThreadMainLogic()
      } catch safely {
        case e: InterruptedException =>
          logger.logInfo("DP Thread exits")
        case e =>
          val error = WorkflowRuntimeError(e, "DP Thread internal logic")
          logger.logError(error)
        // dp thread will stop here
      }
    }
  })

  /** provide API for actor to get stats of this operator
    * @return (input tuple count, output tuple count)
    */
  def collectStatistics(): (Long, Long) = (inputTupleCount, outputTupleCount)

  /** provide API for actor to get current input tuple of this operator
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

  /** process currentInputTuple through operator logic.
    * this function is only called by the DP thread
    * @return an iterator of output tuples
    */
  private[this] def processInputTuple(): Iterator[ITuple] = {
    var outputIterator: Iterator[ITuple] = null
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
    var outputTuple: ITuple = null
    try {
      outputTuple = currentOutputIterator.next
    } catch safely {
      case e =>
        // invalidate current output tuple
        outputTuple = null
        // also invalidate outputIterator
        currentOutputIterator = null
        // forward input tuple to the user and pause DP thread
        handleOperatorException(e)
    }
    if (outputTuple != null) {
      if (breakpointManager.evaluateTuple(outputTuple)) {
        pauseManager.pause()
      } else {
        outputTupleCount += 1
        batchProducer.passTupleToDownstream(outputTuple)
      }
    }
  }

  /** Provide main functionality of data processing
    * @throws Exception (from engine code only)
    */
  @throws[Exception]
  private[this] def runDPThreadMainLogic(): Unit = {
    // main DP loop
    while (!isCompleted) {
      // take the next data element from internal queue, blocks if not available.
      dataDeque.take() match {
        case InputTuple(tuple) =>
          currentInputTuple = Left(tuple)
          handleInputTuple()
        case SenderChangeMarker(link) =>
          currentInputLink = link
        case EndMarker =>
          currentInputTuple = Right(InputExhausted())
          handleInputTuple()
          if (currentInputLink != null) {
            asyncRPCClient.send(LinkCompleted(currentInputLink), ActorVirtualIdentity.Controller)
          }
        case EndOfAllMarker =>
          // end of processing, break DP loop
          isCompleted = true
          batchProducer.emitEndOfUpstream()
        case UnblockForControlCommands =>
          processControlCommandsDuringExecution()
      }
    }
    // Send Completed signal to worker actor.
    logger.logInfo(s"${operator.toString} completed")
    asyncRPCClient.send(WorkerExecutionCompleted(), ActorVirtualIdentity.Controller)
    stateManager.transitTo(Completed)
    processControlCommandsAfterCompletion()
  }

  private[this] def handleOperatorException(e: Throwable): Unit = {
    if (currentInputTuple.isLeft) {
      asyncRPCClient.send(
        LocalOperatorException(currentInputTuple.left.get, e),
        ActorVirtualIdentity.Controller
      )
    } else {
      asyncRPCClient.send(
        LocalOperatorException(ITuple("input exhausted"), e),
        ActorVirtualIdentity.Controller
      )
    }
    e.printStackTrace()
    pauseManager.pause()
  }

  private[this] def handleInputTuple(): Unit = {
    // process controls before processing the input tuple.
    processControlCommandsDuringExecution()
    // if the input tuple is not a dummy tuple, process it
    // TODO: make sure this dummy batch feature works with fault tolerance
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

  def shutdown(): Unit = {
    dpThread.cancel(true) // interrupt
    operator.close() // close operator
    dpThreadExecutor.shutdownNow() // destroy thread
  }

  private[this] def outputAvailable(outputIterator: Iterator[ITuple]): Boolean = {
    try {
      outputIterator != null && outputIterator.hasNext
    } catch safely {
      case e =>
        handleOperatorException(e)
        false
    }
  }

  private[this] def processControlCommandsDuringExecution(): Unit = {
    while (!controlQueue.isEmpty || pauseManager.isPaused) {
      // if paused, dp thread will wait for resume control command here.
      takeOneControlCommandAndProcess()
    }
  }

  private[this] def processControlCommandsAfterCompletion(): Unit = {
    while (true) {
      takeOneControlCommandAndProcess()
    }
  }

  private[this] def takeOneControlCommandAndProcess(): Unit = {
    val (cmd, from) = controlQueue.take()
    cmd match {
      case invocation: ControlInvocation =>
        asyncRPCServer.logControlInvocation(invocation, from)
        asyncRPCServer.receive(invocation, from.asInstanceOf[ActorVirtualIdentity])
      case ret: ReturnPayload =>
        asyncRPCClient.logControlReply(ret, from)
        asyncRPCClient.fulfillPromise(ret)
    }
  }

}
