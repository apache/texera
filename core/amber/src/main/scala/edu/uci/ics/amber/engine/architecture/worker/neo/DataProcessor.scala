package edu.uci.ics.amber.engine.architecture.worker.neo

import java.util.concurrent.Executors

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  ControlOutputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.worker.BreakpointSupport
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue._
import edu.uci.ics.amber.engine.common.amberexception.BreakpointException
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage.LocalBreakpointTriggered
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.ExecutionCompleted
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, InputExhausted, WorkflowLogger}

class DataProcessor( // dependencies:
    operator: IOperatorExecutor, // core logic
    controlOutputChannel: ControlOutputPort, // to send controls to main thread
    batchProducer: TupleToBatchConverter, // to send output tuples
    pauseManager: PauseManager // to pause/resume
) extends WorkerInternalQueue { // TODO: make breakpointSupport as a module

  protected val logger: WorkflowLogger = WorkflowLogger("DataProcessor")
  // dp thread stats:
  // TODO: add another variable for recovery index instead of using the counts below.
  private var inputTupleCount = 0L
  private var outputTupleCount = 0L
  private var currentInputTuple: Either[ITuple, InputExhausted] = _
  private var currentSenderRef: Int = -1
  private var isCompleted = false

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
      // TODO: check breakpoint here
    } catch {
      case bp: BreakpointException =>
        pauseManager.pause()
        controlOutputChannel.sendTo(VirtualIdentity.Self, LocalBreakpointTriggered())
      case e: Exception =>
        handleOperatorException(e, isInput = true)
    }
    if (outputTuple != null) {
      outputTupleCount += 1
      batchProducer.passTupleToDownstream(outputTuple)
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
    controlOutputChannel.sendTo(VirtualIdentity.Self, ExecutionCompleted())
  }

  private[this] def handleOperatorException(e: Exception, isInput: Boolean): Unit = {
    pauseManager.pause()
    controlOutputChannel.sendTo(VirtualIdentity.Self, LocalBreakpointTriggered())
  }

  private[this] def handleInputTuple(): Unit = {
    // check pause before processing the input tuple.
    pauseManager.checkForPause()
    // if the input tuple is not a dummy tuple, process it
    // TODO: make sure this dummy batch feature works with fault tolerance
    if (currentInputTuple != null) {
      // pass input tuple to operator logic.
      val outputIterator = processInputTuple()
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

}
