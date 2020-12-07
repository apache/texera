package edu.uci.ics.amber.engine.architecture.worker.neo

import java.util.concurrent.Executors

import akka.actor.ActorRef
import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.ExceptionBreakpoint
import edu.uci.ics.amber.engine.architecture.worker.BreakpointSupport
import edu.uci.ics.amber.engine.common.amberexception.BreakpointException
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage.LocalBreakpointTriggered
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.ExecutionCompleted
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, InputExhausted}

class DataProcessor( // dependencies:
    operator: IOperatorExecutor, // core logic
    tupleInput: BatchToTupleConverter, // to get input tuples
    tupleOutput: TupleToBatchConverter, // to send output tuples
    pauseManager: PauseManager, // to pause/resume
    self: ActorRef // to notify main actor
) extends BreakpointSupport { // TODO: make breakpointSupport as a module

  // dp thread stats:
  private var inputTupleCount = 0L
  private var outputTupleCount = 0L
  private var currentInputTuple: Either[ITuple, InputExhausted] = _

  // initialize dp thread upon construction
  Executors.newSingleThreadExecutor.submit(new Runnable() {
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

  /** process currentInputTuple through operator logic.
    * this function is only called by the DP thread
    * @return an iterator of output tuples
    */
  private[this] def processCurrentInputTuple(): Iterator[ITuple] = {
    var outputIterator: Iterator[ITuple] = null
    try {
      outputIterator = operator.processTuple(currentInputTuple, tupleInput.getCurrentInput)
      if (currentInputTuple.isLeft) inputTupleCount += 1
    } catch {
      case e: Exception =>
        handleOperatorException(e, isInput = true)
    }
    outputIterator
  }

  /** transfer one tuple from iterator to downstream.
    * this function is only called by the DP thread
    * @param outputIterator
    */
  private[this] def outputOneTuple(outputIterator: Iterator[ITuple]): Unit = {
    var outputTuple: ITuple = null
    try {
      outputTuple = outputIterator.next
    } catch {
      case e: Exception =>
        handleOperatorException(e, isInput = true)
    }
    if (outputTuple != null) {
      try {
        outputTupleCount += 1
        tupleOutput.transferTuple(outputTuple, outputTupleCount)
      } catch {
        case bp: BreakpointException =>
          pauseManager.pause()
          self ! LocalBreakpointTriggered // TODO: apply FIFO & exactly-once protocol here
        case e: Exception =>
          handleOperatorException(e, isInput = false)
      }
    }
  }

  /** Provide main functionality of data processing
    * @throws Exception (from engine code only)
    */
  @throws[Exception]
  private[this] def runDPThreadMainLogic(): Unit = {
    // main DP loop: runs until all upstreams exhaust.
    while (!tupleInput.isAllUpstreamsExhausted) {
      // take the next input tuple from tupleInput, blocks if no tuple available.
      currentInputTuple = tupleInput.getNextInputTuple
      // check pause before processing the input tuple.
      pauseManager.checkForPause()
      // if the input tuple is not a dummy tuple, process it
      // TODO: make sure this dummy batch feature works with fault tolerance
      if (currentInputTuple != null) {
        // pass input tuple to operator logic.
        val outputIterator = processCurrentInputTuple()
        // check pause before outputting tuples.
        pauseManager.checkForPause()
        // output loop: take one tuple from iterator at a time.
        while (outputIterator != null && outputIterator.hasNext) {
          // send tuple to downstream.
          outputOneTuple(outputIterator)
          // check pause after one tuple has been outputted.
          pauseManager.checkForPause()
        }
      }
    }
    // Send Completed signal to worker actor.
    self ! ExecutionCompleted // TODO: apply FIFO & exactly-once protocol here
  }

  // For compatibility, we use old breakpoint handling logic
  // TODO: remove this when we refactor breakpoints
  private[this] def assignExceptionBreakpoint(
      faultedTuple: ITuple,
      e: Exception,
      isInput: Boolean
  ): Unit = {
    breakpoints(0).triggeredTuple = faultedTuple
    breakpoints(0).asInstanceOf[ExceptionBreakpoint].error = e
    breakpoints(0).triggeredTupleId = outputTupleCount
    breakpoints(0).isInput = isInput
  }

  private[this] def handleOperatorException(e: Exception, isInput: Boolean): Unit = {
    pauseManager.pause()
    assignExceptionBreakpoint(currentInputTuple.left.getOrElse(null), e, isInput)
    self ! LocalBreakpointTriggered // TODO: apply FIFO & exactly-once protocol here
  }

}
