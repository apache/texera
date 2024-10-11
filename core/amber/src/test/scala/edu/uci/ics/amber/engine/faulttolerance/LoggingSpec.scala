package edu.uci.ics.amber.engine.faulttolerance

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionCompletedHandler.WorkerExecutionCompleted
import edu.uci.ics.amber.engine.architecture.logreplay.{ReplayLogManager, ReplayLogRecord}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.ambermessage.{
  DataFrame,
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload
}
import edu.uci.ics.amber.engine.common.model.tuple.{AttributeType, Schema, TupleLike}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}
import edu.uci.ics.amber.engine.common.workflow.{PhysicalLink, PortIdentity}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.net.URI

class LoggingSpec
    extends TestKit(ActorSystem("LoggingSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {
  private val identifier1 = ActorVirtualIdentity("Worker:WF1-E1-op-layer-1")
  private val identifier2 = ActorVirtualIdentity("Worker:WF1-E1-op-layer-2")
  private val operatorIdentity = OperatorIdentity("testOperator")
  private val physicalOpId1 = PhysicalOpIdentity(operatorIdentity, "1st-layer")
  private val physicalOpId2 = PhysicalOpIdentity(operatorIdentity, "2nd-layer")
  private val mockLink = PhysicalLink(physicalOpId1, PortIdentity(), physicalOpId2, PortIdentity())

  private val mockPolicy =
    OneToOnePartitioning(10, Seq(ChannelIdentity(identifier1, identifier2, isControl = false)))
  val payloadToLog: Array[WorkflowFIFOMessagePayload] = Array(
    ControlInvocation(0, StartWorker()),
    ControlInvocation(0, AddPartitioning(mockLink, mockPolicy)),
    ControlInvocation(0, PauseWorker()),
    ControlInvocation(0, ResumeWorker()),
    DataFrame(
      (0 to 400)
        .map(i =>
          TupleLike(i, i.toString, i.toDouble).enforceSchema(
            Schema
              .builder()
              .add("field1", AttributeType.INTEGER)
              .add("field2", AttributeType.STRING)
              .add("field3", AttributeType.DOUBLE)
              .build()
          )
        )
        .toArray
    ),
    ControlInvocation(0, StartWorkflow()),
    ControlInvocation(0, WorkerExecutionCompleted())
  )

  "determinant logger" should "log processing steps in local storage" in {
    val logStorage = SequentialRecordStorage.getStorage[ReplayLogRecord](
      Some(new URI("ram:///recovery-logs/tmp"))
    )
    logStorage.deleteStorage()
    val logManager = ReplayLogManager.createLogManager(logStorage, "tmpLog", x => {})
    payloadToLog.foreach { payload =>
      val channel = ChannelIdentity(CONTROLLER, SELF, isControl = true)
      val msgOpt = Some(WorkflowFIFOMessage(channel, 0, payload))
      logManager.withFaultTolerant(channel, msgOpt) {
        // do nothing
      }
    }
    logManager.sendCommitted(null)
    logManager.terminate()
    val logRecords = logStorage.getReader("tmpLog").mkRecordIterator().toArray
    logStorage.deleteStorage()
    assert(logRecords.length == 15)
  }

}
