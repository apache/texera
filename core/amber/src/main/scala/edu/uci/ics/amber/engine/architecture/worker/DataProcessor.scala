package edu.uci.ics.amber.engine.architecture.worker

import java.util.concurrent.Executors

import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkCompletedHandler
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkCompletedHandler.LinkCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalOperatorExceptionHandler.LocalOperatorException
import edu.uci.ics.amber.engine.architecture.messaginglayer.TupleToBatchConverter
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{
  DummyInput,
  EndMarker,
  EndOfAllMarker,
  InputTuple,
  SenderChangeMarker
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.{Completed, Running}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, InputExhausted, WorkflowLogger}
import edu.uci.ics.amber.error.WorkflowRuntimeError

class DataProcessor( // dependencies:
    logger: WorkflowLogger, // logger of the worker actor
    operator: IOperatorExecutor, // core logic
    asyncRPCClient: AsyncRPCClient, // to send controls
    batchProducer: TupleToBatchConverter, // to send output tuples
    pauseManager: PauseManager, // to pause/resume
    breakpointManager: BreakpointManager, // to evaluate breakpoints
    stateManager: WorkerStateManager
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
      } catch {
        case e @ (_: Exception | _: AssertionError | _: StackOverflowError | _: OutOfMemoryError) =>
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
    } catch {
      case e @ (_: Exception | _: AssertionError | _: StackOverflowError | _: OutOfMemoryError) =>
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
    } catch {
      case e @ (_: Exception | _: AssertionError | _: StackOverflowError | _: OutOfMemoryError) =>
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
      blockingDeque.take() match {
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
        case DummyInput =>
          // do a pause check
          pauseManager.checkForPause()
      }
    }
    // Send Completed signal to worker actor.
    logger.logInfo(s"${operator.toString} completed")
    stateManager.transitTo(Completed)
    asyncRPCClient.send(WorkerExecutionCompleted(), ActorVirtualIdentity.Controller)
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
    // check pause before processing the input tuple.
    pauseManager.checkForPause()
    // if the input tuple is not a dummy tuple, process it
    // TODO: make sure this dummy batch feature works with fault tolerance
    if (currentInputTuple != null) {
      // pass input tuple to operator logic.
      currentOutputIterator = processInputTuple()
      // check pause before outputting tuples.
      pauseManager.checkForPause()
      // output loop: take one tuple from iterator at a time.
      while (outputAvailable(currentOutputIterator)) {
        // send tuple to downstream.
        outputOneTuple()
        // check pause after one tuple has been outputted.
        pauseManager.checkForPause()
      }
    }
  }

  def shutdown(): Unit = {
    if (stateManager.confirmState(Running)) {
      pauseManager.pause().onSuccess { ret =>
        // this block will be executed inside DP Thread
        dpThread.cancel(true) // try to interrupt the DP Thread
        operator.close() // close operator
        dpThreadExecutor.shutdownNow() // destroy thread
        // ideally, DP thread will block on
        // PauseManager.dpThreadBlocker.get call
        // then get interrupted and exit
      }
    } else {
      // DP thread will be one of the following cases:
      // 1. blocks on blockingDeque.take() because worker is in ready state
      // 2. blocks on PauseManager.dpThreadBlocker.get because worker is paused
      // note that in above cases, an interrupt exception will be thrown
      // 3. completed
      dpThread.cancel(true) // interrupt
      operator.close()// close operator
      dpThreadExecutor.shutdownNow() // destroy thread
    }
  }

  private[this] def outputAvailable(outputIterator: Iterator[ITuple]): Boolean = {
    try {
      outputIterator != null && outputIterator.hasNext
    } catch {
      case e: Exception =>
        handleOperatorException(e)
        false
    }
  }

}
