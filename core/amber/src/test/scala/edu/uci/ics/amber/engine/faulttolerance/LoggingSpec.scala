package edu.uci.ics.amber.engine.faulttolerance

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, TestKit}
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema, TupleLike}
import edu.uci.ics.amber.engine.architecture.logreplay.{ReplayLogManager, ReplayLogRecord}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AddPartitioningRequest,
  AsyncRPCContext,
  EmptyRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceGrpc.METHOD_WORKER_EXECUTION_COMPLETED
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.{
  METHOD_ADD_PARTITIONING,
  METHOD_PAUSE_WORKER,
  METHOD_RESUME_WORKER,
  METHOD_START_WORKER
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.OneToOnePartitioning
import edu.uci.ics.amber.engine.common.ambermessage.{
  DataFrame,
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}
import edu.uci.ics.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.amber.core.workflow.{PhysicalLink, PortIdentity}
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
    ControlInvocation(
      METHOD_START_WORKER,
      EmptyRequest(),
      AsyncRPCContext(CONTROLLER, identifier1),
      0
    ),
    ControlInvocation(
      METHOD_ADD_PARTITIONING,
      AddPartitioningRequest(mockLink, mockPolicy),
      AsyncRPCContext(CONTROLLER, identifier1),
      0
    ),
    ControlInvocation(
      METHOD_PAUSE_WORKER,
      EmptyRequest(),
      AsyncRPCContext(CONTROLLER, identifier1),
      0
    ),
    ControlInvocation(
      METHOD_RESUME_WORKER,
      EmptyRequest(),
      AsyncRPCContext(CONTROLLER, identifier1),
      0
    ),
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
    ControlInvocation(
      METHOD_START_WORKER,
      EmptyRequest(),
      AsyncRPCContext(CONTROLLER, identifier1),
      0
    ),
    ControlInvocation(
      METHOD_WORKER_EXECUTION_COMPLETED,
      EmptyRequest(),
      AsyncRPCContext(identifier1, CONTROLLER),
      0
    )
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
