package edu.uci.ics.amber.engine.e2e

import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.{
  ConditionalGlobalBreakpoint,
  CountGlobalBreakpoint
}
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage.{
  Ack,
  ModifyLogic,
  Pause,
  Resume,
  Start
}
import edu.uci.ics.amber.engine.common.ambermessage.ControllerMessage.{
  AckedControllerInitialization,
  PassBreakpointTo,
  ReportState
}
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.DataMessage
import edu.uci.ics.amber.engine.common.ambertag.{OperatorIdentifier, WorkflowTag}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.Constants
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.controller.{
  Controller,
  ControllerEventListener,
  ControllerState
}
import edu.uci.ics.texera.web.model.request.{ExecuteWorkflowRequest, TexeraWebSocketRequest}
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource
import edu.uci.ics.texera.workflow.common.{Utils, WorkflowContext}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{
  BreakpointInfo,
  OperatorLink,
  WorkflowCompiler,
  WorkflowInfo
}
import edu.uci.ics.texera.workflow.operators.sink.SimpleSinkOpDesc
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._
import scala.util.Random

class DataProcessingSpec
    extends TestKit(ActorSystem("DataProcessingSpec"))
    with ImplicitSender
    with FlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def beforeAll: Unit = {
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }
  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  def expectCompletedAfterExecution(workflowJson: String): Unit = {
    val parent = TestProbe()
    val context = new WorkflowContext
    context.workflowID = "workflow-test"
    val objectMapper = Utils.objectMapper
    val request =
      objectMapper.readValue(workflowJson, classOf[ExecuteWorkflowRequest])
    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(request.operators, request.links, request.breakpoints),
      context
    )
    texeraWorkflowCompiler.init()
    val workflow = texeraWorkflowCompiler.amberWorkflow
    val workflowTag = WorkflowTag.apply("workflow-test")

    val controller = parent.childActorOf(
      Controller.props(workflowTag, workflow, false, ControllerEventListener(), 100)
    )
    controller ! AckedControllerInitialization
    parent.expectMsg(30.seconds, ReportState(ControllerState.Ready))
    controller ! Start
    parent.expectMsg(ReportState(ControllerState.Running))
    parent.expectMsg(1.minute, ReportState(ControllerState.Completed))
    parent.ref ! PoisonPill
  }

  "Engine" should "execute headerlessCsv->sink workflow normally" in {
    expectCompletedAfterExecution(WorkflowJSONExamples.headerlessCsvToSink)
  }

  "Engine" should "execute headerlessCsv->keyword->sink workflow normally" in {
    expectCompletedAfterExecution(WorkflowJSONExamples.headerlessCsvToKeywordToSink)
  }

  "Engine" should "execute csv->sink workflow normally" in {
    expectCompletedAfterExecution(WorkflowJSONExamples.csvToSink)
  }

  "Engine" should "execute csv->keyword->sink workflow normally" in {
    expectCompletedAfterExecution(WorkflowJSONExamples.csvToKeywordToSink)
  }

  "Engine" should "execute csv->keyword->count->sink workflow normally" in {
    expectCompletedAfterExecution(WorkflowJSONExamples.csvToKeywordToCountToSink)
  }

}
