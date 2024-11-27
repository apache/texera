package edu.uci.ics.amber.engine.e2e

import akka.actor.{ActorSystem, Props}
import akka.serialization.SerializationExtension
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.twitter.util.{Await, Promise}
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.core.storage.result.OpResultStorage
import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ExecutionStateUpdate}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmptyRequest
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.COMPLETED
import edu.uci.ics.amber.engine.common.AmberRuntime
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.operator.{LogicalOp, TestOperators}
import edu.uci.ics.amber.workflow.PortIdentity
import edu.uci.ics.texera.workflow.LogicalLink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._

class PauseSpec
    extends TestKit(ActorSystem("PauseSpec", AmberRuntime.akkaConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)

  val logger = Logger("PauseSpecLogger")

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    AmberRuntime.serde = SerializationExtension(system)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def shouldPause(
      operators: List[LogicalOp],
      links: List[LogicalLink]
  ): Unit = {
    val resultStorage = new OpResultStorage()
    val workflow = TestUtils.buildWorkflow(operators, links, resultStorage, new WorkflowContext())
    val client =
      new AmberClient(
        system,
        workflow.context,
        workflow.physicalPlan,
        resultStorage,
        ControllerConfig.default,
        error => {}
      )
    val completion = Promise[Unit]()
    client
      .registerCallback[ExecutionStateUpdate](evt => {
        if (evt.state == COMPLETED) {
          completion.setDone()
        }
      })
    Await.result(client.controllerInterface.startWorkflow(EmptyRequest(), ()))
    Await.result(client.controllerInterface.pauseWorkflow(EmptyRequest(), ()))
    Thread.sleep(4000)
    Await.result(client.controllerInterface.resumeWorkflow(EmptyRequest(), ()))
    Thread.sleep(400)
    Await.result(client.controllerInterface.pauseWorkflow(EmptyRequest(), ()))
    Thread.sleep(4000)
    Await.result(client.controllerInterface.resumeWorkflow(EmptyRequest(), ()))
    Await.result(completion)
  }

  "Engine" should "be able to pause csv->sink workflow" in {
    val csvOpDesc = TestOperators.mediumCsvScanOpDesc()
    val sink = TestOperators.sinkOpDesc()
    logger.info(s"csv-id ${csvOpDesc.operatorIdentifier}, sink-id ${sink.operatorIdentifier}")
    shouldPause(
      List(csvOpDesc, sink),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(),
          sink.operatorIdentifier,
          PortIdentity()
        )
      )
    )
  }

  "Engine" should "be able to pause csv->keyword->sink workflow" in {
    val csvOpDesc = TestOperators.mediumCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val sink = TestOperators.sinkOpDesc()
    logger.info(
      s"csv-id ${csvOpDesc.operatorIdentifier}, keyword-id ${keywordOpDesc.operatorIdentifier}, sink-id ${sink.operatorIdentifier}"
    )
    shouldPause(
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
      )
    )
  }

}
