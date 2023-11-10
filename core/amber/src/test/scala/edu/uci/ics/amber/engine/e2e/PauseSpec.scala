package edu.uci.ics.amber.engine.e2e

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import com.twitter.util.{Await, Promise}
import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.WorkflowCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{OperatorLink, OperatorPort}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._

class PauseSpec
    extends TestKit(ActorSystem("PauseSpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)

  val logger = Logger("PauseSpecLogger")

  override def beforeAll: Unit = {
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def shouldPause(
      operators: List[OperatorDescriptor],
      links: List[OperatorLink]
  ): Unit = {
    val client =
      new AmberClient(
        system,
        Utils.buildWorkflow(operators, links),
        ControllerConfig.default,
        error => {}
      )
    val completion = Promise[Unit]
    client
      .registerCallback[WorkflowCompleted](evt => {
        completion.setDone()
      })
    Await.result(client.sendAsync(StartWorkflow()))
    Await.result(client.sendAsync(PauseWorkflow()))
    Thread.sleep(4000)
    Await.result(client.sendAsync(ResumeWorkflow()))
    Thread.sleep(400)
    Await.result(client.sendAsync(PauseWorkflow()))
    Thread.sleep(4000)
    Await.result(client.sendAsync(ResumeWorkflow()))
    Await.result(completion)
  }

  "Engine" should "be able to pause csv->sink workflow" in {
    val csvOpDesc = TestOperators.mediumCsvScanOpDesc()
    val sink = TestOperators.sinkOpDesc()
    logger.info(s"csv-id ${csvOpDesc.operatorID}, sink-id ${sink.operatorID}")
    shouldPause(
      List(csvOpDesc, sink),
      List(
        OperatorLink(OperatorPort(csvOpDesc.operatorID, 0), OperatorPort(sink.operatorID, 0))
      )
    )
  }

  "Engine" should "be able to pause csv->keyword->sink workflow" in {
    val csvOpDesc = TestOperators.mediumCsvScanOpDesc()
    val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
    val sink = TestOperators.sinkOpDesc()
    logger.info(
      s"csv-id ${csvOpDesc.operatorID}, keyword-id ${keywordOpDesc.operatorID}, sink-id ${sink.operatorID}"
    )
    shouldPause(
      List(csvOpDesc, keywordOpDesc, sink),
      List(
        OperatorLink(
          OperatorPort(csvOpDesc.operatorID, 0),
          OperatorPort(keywordOpDesc.operatorID, 0)
        ),
        OperatorLink(OperatorPort(keywordOpDesc.operatorID, 0), OperatorPort(sink.operatorID, 0))
      )
    )
  }

}
