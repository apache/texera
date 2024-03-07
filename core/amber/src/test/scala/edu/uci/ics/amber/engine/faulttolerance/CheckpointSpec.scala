package edu.uci.ics.amber.engine.faulttolerance

import akka.actor.{ActorSystem, Props}
import akka.serialization.SerializationExtension
import com.twitter.util.{Await, Duration}
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ControllerProcessor}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.TakeGlobalCheckpointHandler.TakeGlobalCheckpoint
import edu.uci.ics.amber.engine.architecture.worker.DataProcessor
import edu.uci.ics.amber.engine.common.{AmberUtils, CheckpointState}
import edu.uci.ics.amber.engine.common.SerializedState.{CP_STATE_KEY, DP_STATE_KEY}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelMarkerIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.amber.engine.e2e.TestOperators
import edu.uci.ics.amber.engine.e2e.TestUtils.buildWorkflow
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.LogicalLink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.net.URI
import java.util.UUID

class CheckpointSpec extends AnyFlatSpecLike with BeforeAndAfterAll {

  var system: ActorSystem = _
  override def beforeAll(): Unit = {
    system = ActorSystem("Amber", AmberUtils.akkaConfig)
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    AmberUtils.serde = SerializationExtension(system)
  }

  "Default controller state" should "be serializable" in {
    val resultStorage = new OpResultStorage()
    val headerlessCsvOpDesc = TestOperators.headerlessSmallCsvScanOpDesc()
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(headerlessCsvOpDesc, sink),
      List(
        LogicalLink(
          headerlessCsvOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage
    )
    val cp =
      new ControllerProcessor(
        workflow.context,
        resultStorage,
        ControllerConfig.default,
        CONTROLLER,
        msg => {}
      )
    val chkpt = new CheckpointState()
    chkpt.save(CP_STATE_KEY, cp)
  }

  "Default worker state" should "be serializable" in {
    val dp = new DataProcessor(SELF, msg => {})
    val chkpt = new CheckpointState()
    chkpt.save(DP_STATE_KEY, dp)
  }

  "Workflow " should "take global checkpoint" in {
    val resultStorage = new OpResultStorage()
    val csvOpDesc = TestOperators.mediumCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val sink = TestOperators.sinkOpDesc()
    val workflow = buildWorkflow(
      List(csvOpDesc, keywordOpDesc, sink),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(),
          keywordOpDesc.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          keywordOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      ),
      resultStorage
    )
    val client = new AmberClient(
      system,
      workflow.context,
      workflow.physicalPlan,
      resultStorage,
      ControllerConfig.default,
      error => {}
    )
    Await.result(client.sendAsync(StartWorkflow()))
    Thread.sleep(100)
    Await.result(client.sendAsync(PauseWorkflow()))
    val checkpointId = ChannelMarkerIdentity(s"Checkpoint_${UUID.randomUUID().toString}")
    val uri = new URI("ram:///recovery-logs/tmp")
    Await.result(
      client.sendAsync(TakeGlobalCheckpoint(estimationOnly = false, checkpointId, uri)),
      Duration.fromSeconds(30)
    )
  }

}
