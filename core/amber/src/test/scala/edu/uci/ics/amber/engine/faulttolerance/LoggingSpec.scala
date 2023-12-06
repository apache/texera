package edu.uci.ics.amber.engine.faulttolerance

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.architecture.logreplay.ReplayLogManager
import edu.uci.ics.amber.engine.architecture.logreplay.storage.ReplayLogStorage
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.ambermessage.{
  ChannelID,
  DataFrame,
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

class LoggingSpec
    extends TestKit(ActorSystem("LoggingSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  private val identifier2 = ActorVirtualIdentity("worker-2")
  private val operatorIdentity = OperatorIdentity("testWorkflow", "testOperator")
  private val layerId1 =
    LayerIdentity(operatorIdentity.workflow, operatorIdentity.operator, "1st-layer")
  private val layerId2 =
    LayerIdentity(operatorIdentity.workflow, operatorIdentity.operator, "2nd-layer")
  private val mockLink = LinkIdentity(layerId1, 0, layerId2, 0)

  private val mockPolicy = OneToOnePartitioning(10, Array(identifier2))
  val payloadToLog: Array[WorkflowFIFOMessagePayload] = Array(
    ControlInvocation(0, StartWorker()),
    ControlInvocation(0, AddPartitioning(mockLink, mockPolicy)),
    ControlInvocation(0, PauseWorker()),
    ControlInvocation(0, ResumeWorker()),
    DataFrame((0 to 400).map(i => ITuple(i, i.toString, i.toDouble)).toArray),
    ControlInvocation(0, StartWorkflow()),
    ControlInvocation(0, WorkerExecutionCompleted())
  )

  "determinant logger" should "log processing steps in local storage" in {
    val tempLogFileName = "tempLogFile"
    val logStorage = ReplayLogStorage.getLogStorage("local", tempLogFileName)
    logStorage.deleteLog()
    val logManager = ReplayLogManager.createLogManager(logStorage, x => {})
    payloadToLog.foreach { payload =>
      val channel = ChannelID(CONTROLLER, SELF, isControl = true)
      val msgOpt = Some(WorkflowFIFOMessage(channel, 0, payload))
      logManager.withFaultTolerant(channel, msgOpt) {
        // do nothing
      }
    }
    logManager.sendCommitted(null)
    logManager.terminate()
    val logRecords = logStorage.getReader.mkLogRecordIterator().toArray
    logStorage.deleteLog()
    assert(logRecords.length == 15)
  }

}
