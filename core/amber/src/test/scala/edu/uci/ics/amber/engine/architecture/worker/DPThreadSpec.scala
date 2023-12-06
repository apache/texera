package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.logreplay.ReplayLogManager
import edu.uci.ics.amber.engine.architecture.logreplay.storage.ReplayLogStorage
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{OpExecConfig, OpExecInitInfo}
import edu.uci.ics.amber.engine.architecture.messaginglayer.WorkerTimerService
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  FIFOMessageElement,
  TimerBasedControlElement
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, DataFrame, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.LinkedBlockingQueue

class DPThreadSpec extends AnyFlatSpec with MockFactory {

  private val identifier: ActorVirtualIdentity = ActorVirtualIdentity("DP mock")
  private val senderID: ActorVirtualIdentity = ActorVirtualIdentity("mock sender")
  private val dataChannelID = ChannelID(senderID, identifier, false)
  private val controlChannelID = ChannelID(senderID, identifier, true)
  private val operator = mock[OperatorExecutor]
  private val operatorIdentity = OperatorIdentity("testWorkflow", "testOperator")
  private val layerId1 =
    LayerIdentity(operatorIdentity.workflow, operatorIdentity.operator, "1st-layer")
  private val layerId2 =
    LayerIdentity(operatorIdentity.workflow, operatorIdentity.operator, "1st-layer")
  private val mockLink = LinkIdentity(layerId1, 0, layerId2, 0)
  private val opExecConfig = OpExecConfig
    .oneToOneLayer(operatorIdentity, OpExecInitInfo(_ => operator))
    .copy(inputToOrdinalMapping = Map(mockLink -> 0), outputToOrdinalMapping = Map(mockLink -> 0))
  private val tuples: Array[ITuple] = (0 until 5000).map(ITuple(_)).toArray
  private val logStorage = ReplayLogStorage.getLogStorage("none", "log")
  private val logManager: ReplayLogManager = ReplayLogManager.createLogManager(logStorage, x => {})

  "DP Thread" should "handle pause/resume during processing" in {
    val dp = new DataProcessor(identifier, x => {})
    dp.initOperator(0, opExecConfig, Iterator.empty)
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    dp.registerInput(senderID, mockLink)
    dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (dp.adaptiveBatchingMonitor.resumeAdaptiveBatching _).expects().anyNumberOfTimes()
    val dpThread = new DPThread(identifier, dp, logManager, inputQueue)
    dpThread.start()
    tuples.foreach { x =>
      (operator.processTuple _).expects(Left(x), 0, dp.pauseManager, dp.asyncRPCClient)
    }
    val message = WorkflowFIFOMessage(dataChannelID, 0, DataFrame(tuples))
    inputQueue.put(FIFOMessageElement(message))
    inputQueue.put(
      TimerBasedControlElement(ControlInvocation(0, PauseWorker()))
    )
    Thread.sleep(1000)
    assert(dp.pauseManager.isPaused)
    inputQueue.put(TimerBasedControlElement(ControlInvocation(1, ResumeWorker())))
    Thread.sleep(1000)
    while (dp.hasUnfinishedInput) {
      Thread.sleep(100)
    }
  }

  "DP Thread" should "handle pause/resume using fifo messages" in {
    val dp = new DataProcessor(identifier, x => {})
    dp.initOperator(0, opExecConfig, Iterator.empty)
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    dp.registerInput(senderID, mockLink)
    dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (dp.adaptiveBatchingMonitor.resumeAdaptiveBatching _).expects().anyNumberOfTimes()
    val dpThread = new DPThread(identifier, dp, logManager, inputQueue)
    dpThread.start()
    tuples.foreach { x =>
      (operator.processTuple _).expects(Left(x), 0, dp.pauseManager, dp.asyncRPCClient)
    }
    val message = WorkflowFIFOMessage(dataChannelID, 0, DataFrame(tuples))
    val pauseControl = WorkflowFIFOMessage(controlChannelID, 0, ControlInvocation(0, PauseWorker()))
    val resumeControl =
      WorkflowFIFOMessage(controlChannelID, 1, ControlInvocation(1, ResumeWorker()))
    inputQueue.put(FIFOMessageElement(message))
    inputQueue.put(
      FIFOMessageElement(pauseControl)
    )
    Thread.sleep(1000)
    assert(dp.pauseManager.isPaused)
    inputQueue.put(FIFOMessageElement(resumeControl))
    Thread.sleep(1000)
    while (dp.hasUnfinishedInput) {
      Thread.sleep(100)
    }
  }

  "DP Thread" should "handle multiple batches from multiple sources" in {
    val dp = new DataProcessor(identifier, x => {})
    dp.initOperator(0, opExecConfig, Iterator.empty)
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    val anotherSender = ActorVirtualIdentity("another")
    dp.registerInput(senderID, mockLink)
    dp.registerInput(anotherSender, mockLink)
    dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (dp.adaptiveBatchingMonitor.resumeAdaptiveBatching _).expects().anyNumberOfTimes()
    val dpThread = new DPThread(identifier, dp, logManager, inputQueue)
    dpThread.start()
    tuples.foreach { x =>
      (operator.processTuple _).expects(Left(x), 0, dp.pauseManager, dp.asyncRPCClient)
    }
    val dataChannelID2 = ChannelID(anotherSender, identifier, false)
    val message1 = WorkflowFIFOMessage(dataChannelID, 0, DataFrame(tuples.slice(0, 100)))
    val message2 = WorkflowFIFOMessage(dataChannelID, 1, DataFrame(tuples.slice(100, 200)))
    val message3 = WorkflowFIFOMessage(dataChannelID2, 0, DataFrame(tuples.slice(300, 1000)))
    val message4 = WorkflowFIFOMessage(dataChannelID, 2, DataFrame(tuples.slice(200, 300)))
    val message5 = WorkflowFIFOMessage(dataChannelID2, 1, DataFrame(tuples.slice(1000, 5000)))
    inputQueue.put(FIFOMessageElement(message1))
    inputQueue.put(FIFOMessageElement(message2))
    inputQueue.put(FIFOMessageElement(message3))
    inputQueue.put(FIFOMessageElement(message4))
    inputQueue.put(FIFOMessageElement(message5))
    Thread.sleep(1000)
    while (dp.hasUnfinishedInput) {
      Thread.sleep(100)
    }
  }

  "DP Thread" should "write determinant logs to local storage while processing" in {
    val dp = new DataProcessor(identifier, x => {})
    dp.initOperator(0, opExecConfig, Iterator.empty)
    val inputQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    val anotherSender = ActorVirtualIdentity("another")
    dp.registerInput(senderID, mockLink)
    dp.registerInput(anotherSender, mockLink)
    dp.adaptiveBatchingMonitor = mock[WorkerTimerService]
    (dp.adaptiveBatchingMonitor.resumeAdaptiveBatching _).expects().anyNumberOfTimes()
    val logStorage = ReplayLogStorage.getLogStorage("local", "DPSpecTemp")
    logStorage.deleteLog()
    val logManager: ReplayLogManager = ReplayLogManager.createLogManager(logStorage, x => {})
    val dpThread = new DPThread(identifier, dp, logManager, inputQueue)
    dpThread.start()
    tuples.foreach { x =>
      (operator.processTuple _).expects(Left(x), 0, dp.pauseManager, dp.asyncRPCClient)
    }
    val dataChannelID2 = ChannelID(anotherSender, identifier, false)
    val message1 = WorkflowFIFOMessage(dataChannelID, 0, DataFrame(tuples.slice(0, 100)))
    val message2 = WorkflowFIFOMessage(dataChannelID, 1, DataFrame(tuples.slice(100, 200)))
    val message3 = WorkflowFIFOMessage(dataChannelID2, 0, DataFrame(tuples.slice(300, 1000)))
    val message4 = WorkflowFIFOMessage(dataChannelID, 2, DataFrame(tuples.slice(200, 300)))
    val message5 = WorkflowFIFOMessage(dataChannelID2, 1, DataFrame(tuples.slice(1000, 5000)))
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
    val logs = logStorage.getReader.mkLogRecordIterator().toArray
    logStorage.deleteLog()
    assert(logs.length > 1)
  }

}
