package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.logreplay.{ReplayLogManager, ReplayLogRecord}
import edu.uci.ics.amber.engine.architecture.messaginglayer.WorkerTimerService
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  FIFOMessageElement,
  TimerBasedControlElement
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI
import java.util.concurrent.LinkedBlockingQueue

class DPThreadSpec extends AnyFlatSpec with MockFactory {

  private val workerId: ActorVirtualIdentity = ActorVirtualIdentity("DP mock")
  private val senderWorkerId: ActorVirtualIdentity = ActorVirtualIdentity("mock sender")
  private val dataChannelId = ChannelIdentity(senderWorkerId, workerId, isControl = false)
  private val controlChannelId = ChannelIdentity(senderWorkerId, workerId, isControl = true)
  private val executor = mock[OperatorExecutor]
  private val mockInputPortId = PortIdentity()

  private val schema: Schema = Schema.builder().add("field1", AttributeType.INTEGER).build()
  private val tuples: Array[Tuple] = (0 until 5000)
    .map(i => TupleLike(i).enforceSchema(schema))
    .toArray
  private val logStorage = SequentialRecordStorage.getStorage[ReplayLogRecord](None)
  private val logManager: ReplayLogManager =
    ReplayLogManager.createLogManager(logStorage, "none", x => {})

  "DP Thread" should "handle pause/resume during processing" in {
    val dp = new DataProcessor(workerId, x => {})
    dp.executor = executor
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    dp.inputManager.addPort(mockInputPortId, schema)
    dp.inputGateway.getChannel(dataChannelId).setPortId(mockInputPortId)
    dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (dp.adaptiveBatchingMonitor.resumeAdaptiveBatching _).expects().anyNumberOfTimes()
    val dpThread = new DPThread(workerId, dp, logManager, inputQueue)
    dpThread.start()
    tuples.foreach { x =>
      (
          (
              tuple: Tuple,
              input: Int
          ) => executor.processTupleMultiPort(tuple, input)
      )
        .expects(x, 0)
    }
    val message = WorkflowFIFOMessage(dataChannelId, 0, DataFrame(tuples))
    inputQueue.put(FIFOMessageElement(message))
    inputQueue.put(
      TimerBasedControlElement(ControlInvocation(0, PauseWorker()))
    )
    Thread.sleep(1000)
    assert(dp.pauseManager.isPaused)
    inputQueue.put(TimerBasedControlElement(ControlInvocation(1, ResumeWorker())))
    Thread.sleep(1000)
    while (dp.inputManager.hasUnfinishedInput) {
      Thread.sleep(100)
    }
  }

  "DP Thread" should "handle pause/resume using fifo messages" in {
    val dp = new DataProcessor(workerId, x => {})
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    dp.inputManager.addPort(mockInputPortId, schema)
    dp.inputGateway.getChannel(dataChannelId).setPortId(mockInputPortId)
    dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (dp.adaptiveBatchingMonitor.resumeAdaptiveBatching _).expects().anyNumberOfTimes()
    val dpThread = new DPThread(workerId, dp, logManager, inputQueue)
    dp.executor = executor
    dpThread.start()
    tuples.foreach { x =>
      (
          (
              tuple: Tuple,
              input: Int
          ) => executor.processTupleMultiPort(tuple, input)
      )
        .expects(x, 0)
    }
    val message = WorkflowFIFOMessage(dataChannelId, 0, DataFrame(tuples))
    val pauseControl = WorkflowFIFOMessage(controlChannelId, 0, ControlInvocation(0, PauseWorker()))
    val resumeControl =
      WorkflowFIFOMessage(controlChannelId, 1, ControlInvocation(1, ResumeWorker()))
    inputQueue.put(FIFOMessageElement(message))
    inputQueue.put(
      FIFOMessageElement(pauseControl)
    )
    Thread.sleep(1000)
    assert(dp.pauseManager.isPaused)
    inputQueue.put(FIFOMessageElement(resumeControl))
    Thread.sleep(1000)
    while (dp.inputManager.hasUnfinishedInput) {
      Thread.sleep(100)
    }
  }

  "DP Thread" should "handle multiple batches from multiple sources" in {
    val dp = new DataProcessor(workerId, x => {})
    dp.executor = executor
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    val anotherSenderWorkerId = ActorVirtualIdentity("another")
    dp.inputManager.addPort(mockInputPortId, schema)
    dp.inputGateway.getChannel(dataChannelId).setPortId(mockInputPortId)
    dp.inputGateway
      .getChannel(ChannelIdentity(anotherSenderWorkerId, workerId, isControl = false))
      .setPortId(mockInputPortId)
    dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (dp.adaptiveBatchingMonitor.resumeAdaptiveBatching _).expects().anyNumberOfTimes()
    val dpThread = new DPThread(workerId, dp, logManager, inputQueue)
    dpThread.start()
    tuples.foreach { x =>
      (
          (
              tuple: Tuple,
              input: Int
          ) => executor.processTupleMultiPort(tuple, input)
      )
        .expects(x, 0)
    }
    val dataChannelID2 = ChannelIdentity(anotherSenderWorkerId, workerId, isControl = false)
    val message1 = WorkflowFIFOMessage(dataChannelId, 0, DataFrame(tuples.slice(0, 100)))
    val message2 = WorkflowFIFOMessage(dataChannelId, 1, DataFrame(tuples.slice(100, 200)))
    val message3 = WorkflowFIFOMessage(dataChannelID2, 0, DataFrame(tuples.slice(300, 1000)))
    val message4 = WorkflowFIFOMessage(dataChannelId, 2, DataFrame(tuples.slice(200, 300)))
    val message5 = WorkflowFIFOMessage(dataChannelID2, 1, DataFrame(tuples.slice(1000, 5000)))
    inputQueue.put(FIFOMessageElement(message1))
    inputQueue.put(FIFOMessageElement(message2))
    inputQueue.put(FIFOMessageElement(message3))
    inputQueue.put(FIFOMessageElement(message4))
    inputQueue.put(FIFOMessageElement(message5))
    Thread.sleep(1000)
    while (dp.inputManager.hasUnfinishedInput) {
      Thread.sleep(100)
    }
  }

  "DP Thread" should "write determinant logs to local storage while processing" in {
    val dp = new DataProcessor(workerId, x => {})
    dp.executor = executor
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    val anotherSenderWorkerId = ActorVirtualIdentity("another")
    dp.inputManager.addPort(mockInputPortId, schema)
    dp.inputGateway.getChannel(dataChannelId).setPortId(mockInputPortId)
    dp.inputGateway
      .getChannel(ChannelIdentity(anotherSenderWorkerId, workerId, isControl = false))
      .setPortId(mockInputPortId)
    dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (dp.adaptiveBatchingMonitor.resumeAdaptiveBatching _).expects().anyNumberOfTimes()
    val logStorage = SequentialRecordStorage.getStorage[ReplayLogRecord](
      Some(new URI("ram:///recovery-logs/tmp"))
    )
    logStorage.deleteStorage()
    val logManager: ReplayLogManager =
      ReplayLogManager.createLogManager(logStorage, "tmpLog", x => {})
    val dpThread = new DPThread(workerId, dp, logManager, inputQueue)
    dpThread.start()
    tuples.foreach { x =>
      (
          (
              tuple: Tuple,
              input: Int
          ) => executor.processTupleMultiPort(tuple, input)
      )
        .expects(x, 0)
    }
    val dataChannelId2 = ChannelIdentity(anotherSenderWorkerId, workerId, isControl = false)
    val message1 = WorkflowFIFOMessage(dataChannelId, 0, DataFrame(tuples.slice(0, 100)))
    val message2 = WorkflowFIFOMessage(dataChannelId, 1, DataFrame(tuples.slice(100, 200)))
    val message3 = WorkflowFIFOMessage(dataChannelId2, 0, DataFrame(tuples.slice(300, 1000)))
    val message4 = WorkflowFIFOMessage(dataChannelId, 2, DataFrame(tuples.slice(200, 300)))
    val message5 = WorkflowFIFOMessage(dataChannelId2, 1, DataFrame(tuples.slice(1000, 5000)))
    inputQueue.put(FIFOMessageElement(message1))
    inputQueue.put(FIFOMessageElement(message2))
    inputQueue.put(FIFOMessageElement(message3))
    Thread.sleep(1000)
    inputQueue.put(FIFOMessageElement(message4))
    inputQueue.put(FIFOMessageElement(message5))
    Thread.sleep(1000)
    while (logManager.getStep < 4999) {
      Thread.sleep(100)
    }
    logManager.sendCommitted(null) // drain in-mem records to flush
    logManager.terminate()
    val logs = logStorage.getReader("tmpLog").mkRecordIterator().toArray
    logStorage.deleteStorage()
    assert(logs.length > 1)
  }

}
