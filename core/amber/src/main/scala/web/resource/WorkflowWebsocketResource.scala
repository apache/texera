package web.resource

import java.util.concurrent.atomic.AtomicInteger

import engine.architecture.controller.{Controller, ControllerEventListener}
import engine.architecture.principal.PrincipalStatistics
import engine.common.ambermessage.ControlMessage.{ModifyLogic, Pause, Resume, SkipTuple, SkipTupleGivenWorkerRef, Start}
import engine.common.ambermessage.ControllerMessage.{AckedControllerInitialization, PassBreakpointTo}
import engine.common.ambertag.WorkflowTag
import akka.actor.{ActorRef, PoisonPill}
import javax.websocket.server.ServerEndpoint
import javax.websocket._
import texera.common.workflow.{TexeraWorkflow, TexeraWorkflowCompiler}
import texera.common.{TexeraContext, TexeraUtils}
import texera.operators.localscan.LocalCsvFileScanOpDesc
import texera.operators.sink.SimpleSinkOpDesc
import web.TexeraWebApplication
import web.model.event._
import web.model.request._

import scala.collection.mutable

object WorkflowWebsocketResource {

  val nextWorkflowID = new AtomicInteger(0)

  val sessionMap = new mutable.HashMap[String, Session]
  val sessionJobs = new mutable.HashMap[String, (TexeraWorkflowCompiler, ActorRef)]

}

@ServerEndpoint("/wsapi/workflow-websocket")
class WorkflowWebsocketResource {

  final val objectMapper = TexeraUtils.objectMapper

  @OnOpen
  def myOnOpen(session: Session): Unit = {
    WorkflowWebsocketResource.sessionMap.update(session.getId, session)
    println("connection open")
  }

  @OnMessage
  def myOnMsg(session: Session, message: String): Unit = {
    val request = objectMapper.readValue(message, classOf[TexeraWsRequest])
    println(request)
    try {
      request match {
        case helloWorld: HelloWorldRequest =>
          send(session, HelloWorldResponse("hello from texera web server"))
        case execute: ExecuteWorkflowRequest =>
          println(execute)
          executeWorkflow(session, execute)
        case newLogic: ModifyLogicRequest =>
          println(newLogic)
          modifyLogic(session, newLogic)
        case pause: PauseWorkflowRequest =>
          pauseWorkflow(session)
        case resume: ResumeWorkflowRequest =>
          resumeWorkflow(session)
        case kill: KillWorkflowRequest =>
          killWorkflow(session)
        case skipTupleMsg: SkipTupleRequest =>
          skipTuple(session, skipTupleMsg)
        case breakpoint: AddBreakpointRequest =>
          addBreakpoint(session, breakpoint)
      }
    } catch {
      case e: Throwable => {
        send(session, WorkflowErrorEvent(generalErrors = Map("exception" -> e.getMessage)))
        throw e
      }
    }

  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {
    if (WorkflowWebsocketResource.sessionJobs.contains(session.getId)) {
      println(s"session ${session.getId} disconnected, kill its controller actor")
      this.killWorkflow(session)
    }
  }

  def send(session: Session, event: TexeraWsEvent): Unit = {
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(event))
  }

  def addBreakpoint(session: Session, addBreakpoint: AddBreakpointRequest): Unit = {
    val compiler = WorkflowWebsocketResource.sessionJobs(session.getId)._1
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    compiler.addBreakpoint(controller, addBreakpoint.operatorID, addBreakpoint.breakpoint)
  }

  def removeBreakpoint(session: Session, removeBreakpoint: RemoveBreakpointRequest): Unit = {
    throw new UnsupportedOperationException();
  }

  def skipTuple(session: Session, tupleReq: SkipTupleRequest): Unit = {
    val actorPath = tupleReq.actorPath
    val faultedTuple = tupleReq.faultedTuple
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    controller ! SkipTupleGivenWorkerRef(actorPath, faultedTuple.toFaultedTuple())
  }

  def modifyLogic(session: Session, newLogic: ModifyLogicRequest): Unit = {
    val texeraOperator = newLogic.operator
    val (compiler, controller) = WorkflowWebsocketResource.sessionJobs(session.getId)
    compiler.initOperator(texeraOperator)
    controller ! ModifyLogic(texeraOperator.texeraOperatorExecutor)
  }

  def pauseWorkflow(session: Session): Unit = {
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    controller ! Pause
    // workflow paused event will be send after workflow is actually paused
    // the callback function will handle sending the paused event to frontend
  }

  def resumeWorkflow(session: Session): Unit = {
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    controller ! Resume
    send(session, WorkflowResumedEvent())
  }

  def killWorkflow(session: Session): Unit = {
    WorkflowWebsocketResource.sessionJobs(session.getId)._2 ! PoisonPill
    println("workflow killed")
  }

  def executeWorkflow(session: Session, request: ExecuteWorkflowRequest): Unit = {
    val context = new TexeraContext
    val workflowID = Integer.toString(WorkflowWebsocketResource.nextWorkflowID.incrementAndGet)
    context.workflowID = workflowID

    val texeraWorkflowCompiler = new TexeraWorkflowCompiler(
      TexeraWorkflow(request.operators, request.links, request.breakpoints),
      context
    )

    texeraWorkflowCompiler.init()
    val violations = texeraWorkflowCompiler.validate
    if (violations.nonEmpty) {
      send(session, WorkflowErrorEvent(violations))
      return
    }

    val workflow = texeraWorkflowCompiler.amberWorkflow
    val workflowTag = WorkflowTag.apply(workflowID)

    val eventListener = ControllerEventListener(
      workflowCompletedListener = completed => {
        send(session, WorkflowCompletedEvent.apply(completed))
        WorkflowWebsocketResource.sessionJobs.remove(session.getId)
      },
      workflowStatusUpdateListener = statusUpdate => {
        val updateMutable = mutable.HashMap(statusUpdate.operatorStatistics.toSeq: _*)
        val sinkID = texeraWorkflowCompiler.texeraWorkflow.operators
          .find(p => p.isInstanceOf[SimpleSinkOpDesc])
          .get
          .operatorID
        val sinkInputID = texeraWorkflowCompiler.texeraWorkflow.links
          .find(link => link.destination == sinkID)
          .get
          .origin
        if (updateMutable.contains(sinkInputID)) {
          val inputStatistics = updateMutable(sinkInputID)
          val sinkStatistics = PrincipalStatistics(
            inputStatistics.operatorState,
            inputStatistics.aggregatedOutputRowCount,
            inputStatistics.aggregatedOutputRowCount
          )
          updateMutable(sinkID) = sinkStatistics
        }
        send(session, WorkflowStatusUpdateEvent(updateMutable.toMap))
      },
      modifyLogicCompletedListener = _ => {
        send(session, ModifyLogicCompletedEvent())
      },
      breakpointTriggeredListener = breakpointTriggered => {
        send(session, BreakpointTriggeredEvent.apply(breakpointTriggered))
      },
      workflowPausedListener = _ => {
        send(session, WorkflowPausedEvent())
      },
      skipTupleResponseListener = _ => {
        send(session, SkipTupleResponseEvent())
      },
      reportCurrentTuplesListener = report => {
//        send(session, OperatorCurrentTuplesUpdateEvent.apply(report))
      },
      recoveryStartedListener = _ => {
        send(session, RecoveryStartedEvent())
      }
    )

    val controllerActorRef = TexeraWebApplication.actorSystem.actorOf(
      Controller.props(workflowTag, workflow, false, eventListener, 100)
    )
    controllerActorRef ! AckedControllerInitialization
    texeraWorkflowCompiler.initializeBreakpoint(controllerActorRef)
    controllerActorRef ! Start

    WorkflowWebsocketResource.sessionJobs(session.getId) =
      (texeraWorkflowCompiler, controllerActorRef)

    send(session, WorkflowStartedEvent())

  }

}
