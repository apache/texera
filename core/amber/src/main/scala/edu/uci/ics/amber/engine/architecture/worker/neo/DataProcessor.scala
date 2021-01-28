package edu.uci.ics.amber.engine.architecture.worker.neo

import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ExecutionCompletedHandler.ExecutionCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalOperatorExceptionHandler.LocalOperatorException
import edu.uci.ics.amber.engine.architecture.messaginglayer.{ControlOutputPort, TupleToBatchConverter}
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.{DummyInput, EndMarker, EndOfAllMarker, InputTuple, SenderChangeMarker}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, InputExhausted, WorkflowLogger}
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class DataProcessor( // dependencies:
                     operator: IOperatorExecutor, // core logic
                     asyncRPCClient: AsyncRPCClient, // to send controls
                     batchProducer: TupleToBatchConverter, // to send output tuples
                     pauseManager: PauseManager, // to pause/resume
                     breakpointManager: BreakpointManager // to evaluate breakpoints
) extends WorkerInternalQueue { // TODO: make breakpointSupport as a module

  protected val logger: WorkflowLogger = WorkflowLogger("DataProcessor")
  // dp thread stats:
  // TODO: add another variable for recovery index instead of using the counts below.
  private var inputTupleCount = 0L
  private var outputTupleCount = 0L
  private var currentInputTuple: Either[ITuple, InputExhausted] = _
  private var currentSenderRef: Int = -1
  private var currentOutputIterator: Iterator[ITuple] = _
  private var isCompleted = false

  // initialize operator
  operator.open()

  // initialize dp thread upon construction
  private val dpThread = Executors.newSingleThreadExecutor.submit(new Runnable() {
    def run(): Unit = {
      try {
        runDPThreadMainLogic()
      } catch {
        case e: Exception =>
          throw new RuntimeException(e)
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
      outputIterator = operator.processTuple(currentInputTuple, currentSenderRef)
      if (currentInputTuple.isLeft) inputTupleCount += 1
    } catch {
      case e: Exception =>
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
      case e: Exception =>
        // invalidate current output tuple
        outputTuple = null
        // also invalidate outputIterator
        currentOutputIterator = null
        // forward input tuple to the user and pause DP thread
        handleOperatorException(e)
    }
    if (outputTuple != null) {
      if(breakpointManager.evaluateTuple(outputTuple)){
        pauseManager.pause()
      }else{
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
        case SenderChangeMarker(newSenderRef) =>
          currentSenderRef = newSenderRef
        case EndMarker() =>
          currentInputTuple = Right(InputExhausted())
          handleInputTuple()
        case EndOfAllMarker() =>
          // end of processing, break DP loop
          isCompleted = true
          batchProducer.emitEndOfUpstream()
        case DummyInput() =>
          // do a pause check
          pauseManager.checkForPause()
      }
    }
    // Send Completed signal to worker actor.
    logger.logInfo(s"${operator.toString} completed")
    asyncRPCClient.send(ExecutionCompleted(),VirtualIdentity.Controller)
  }

  private[this] def handleOperatorException(e: Exception): Unit = {
    if(currentInputTuple.isLeft){
      asyncRPCClient.send(LocalOperatorException(currentInputTuple.left.get, e),VirtualIdentity.Controller)
    }else{
      asyncRPCClient.send(LocalOperatorException(ITuple("input exhausted"), e), VirtualIdentity.Controller)
    }
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
      while (currentOutputIterator != null && currentOutputIterator.hasNext) {
        // send tuple to downstream.
        outputOneTuple()
        // check pause after one tuple has been outputted.
        pauseManager.checkForPause()
      }
    }
  }


  def shutdown(): Unit ={
    dpThread.cancel(true)
  }

}
