package edu.uci.ics.texera.web.resource.execution

import akka.actor.{ActorRef, PoisonPill}
import com.google.common.collect.EvictingQueue
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.{ConditionalGlobalBreakpoint, CountGlobalBreakpoint}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{BreakpointTriggered, ErrorOccurred, PythonPrintTriggered, WorkflowCompleted, WorkflowStatusUpdate}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.AssignBreakpointHandler.AssignGlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RetryWorkflowHandler.RetryWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.architecture.principal.{OperatorState, OperatorStatistics}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.texera.web.TexeraWebApplication
import edu.uci.ics.texera.web.model.common.FaultedTupleFrontend
import edu.uci.ics.texera.web.model.event.{BreakpointFault, BreakpointTriggeredEvent, PythonPrintTriggeredEvent, TexeraWebSocketEvent, WebWorkflowStatusUpdateEvent, WorkflowExecutionErrorEvent, WorkflowResumedEvent}
import edu.uci.ics.texera.web.model.request.{AddBreakpointRequest, ModifyLogicRequest, RemoveBreakpointRequest, SkipTupleRequest}
import edu.uci.ics.texera.web.resource.Observer
import edu.uci.ics.texera.web.resource.execution.WorkflowRuntimeService.CONSOLE_BUFFER_SIZE
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.{Breakpoint, BreakpointCondition, ConditionBreakpoint, CountBreakpoint, WorkflowInfo}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object WorkflowRuntimeService{
  val CONSOLE_BUFFER_SIZE = 100
}


class WorkflowRuntimeService(workflow:Workflow, controllerObservables: ControllerSubjects) extends BehaviorSubject with LazyLogging {

  class OperatorRuntimeState{
    var stats:OperatorStatistics = OperatorStatistics(OperatorState.Uninitialized, 0, 0)
    val pythonMessages: EvictingQueue[String] = EvictingQueue.create[String](CONSOLE_BUFFER_SIZE)
    val faults:mutable.ArrayBuffer[BreakpointFault] = new ArrayBuffer[BreakpointFault]()
  }

  private var controller:ActorRef = _
  val operatorRuntimeStateMap: mutable.HashMap[String, OperatorRuntimeState] = new mutable.HashMap[String, OperatorRuntimeState]()
  var workflowError:Throwable = _
  def isActive:Boolean = controller != null
  def startWorkflow(): Unit ={
    controller = TexeraWebApplication.actorSystem.actorOf(
        Controller.props(workflow, controllerObservables, ControllerConfig.default)
      )
    controller ! ControlInvocation(AsyncRPCClient.IgnoreReply, StartWorkflow())
  }

  def clearTriggeredBreakpoints(): Unit ={
    operatorRuntimeStateMap.values.foreach{
      state => state.faults.clear()
    }
  }

  def addBreakpoint(
                     operatorID: String,
                     breakpoint: Breakpoint
                   ): Unit = {
    val breakpointID = "breakpoint-" + operatorID + "-" + System.currentTimeMillis()
    breakpoint match {
      case conditionBp: ConditionBreakpoint =>
        val column = conditionBp.column
        val predicate: Tuple => Boolean = conditionBp.condition match {
          case BreakpointCondition.EQ =>
            tuple => {
              tuple.getField(column).toString.trim == conditionBp.value
            }
          case BreakpointCondition.LT =>
            tuple => tuple.getField(column).toString.trim < conditionBp.value
          case BreakpointCondition.LE =>
            tuple => tuple.getField(column).toString.trim <= conditionBp.value
          case BreakpointCondition.GT =>
            tuple => tuple.getField(column).toString.trim > conditionBp.value
          case BreakpointCondition.GE =>
            tuple => tuple.getField(column).toString.trim >= conditionBp.value
          case BreakpointCondition.NE =>
            tuple => tuple.getField(column).toString.trim != conditionBp.value
          case BreakpointCondition.CONTAINS =>
            tuple => tuple.getField(column).toString.trim.contains(conditionBp.value)
          case BreakpointCondition.NOT_CONTAINS =>
            tuple => !tuple.getField(column).toString.trim.contains(conditionBp.value)
        }

        controller ! ControlInvocation(
          AsyncRPCClient.IgnoreReply,
          AssignGlobalBreakpoint(
            new ConditionalGlobalBreakpoint(
              breakpointID,
              tuple => {
                val texeraTuple = tuple.asInstanceOf[Tuple]
                predicate.apply(texeraTuple)
              }
            ),
            operatorID
          )
        )
      case countBp: CountBreakpoint =>
        controller ! ControlInvocation(
          AsyncRPCClient.IgnoreReply,
          AssignGlobalBreakpoint(new CountGlobalBreakpoint(breakpointID, countBp.count), operatorID)
        )
    }
  }

  controllerObservables.breakpointTriggered.subscribe((evt:BreakpointTriggered) => {
    val faults = operatorRuntimeStateMap(evt.operatorID).faults
    for (elem <- evt.report) {
      val actorPath = elem._1._1.toString
      val faultedTuple = elem._1._2
      if (faultedTuple != null) {
        faults += BreakpointFault(actorPath, FaultedTupleFrontend.apply(faultedTuple), elem._2)
      }
    }
    onNext(BreakpointTriggeredEvent(faults.toArray, evt.operatorID))
  })

  controllerObservables.workflowStatusUpdate.subscribe((evt:WorkflowStatusUpdate) => {
    evt.operatorStatistics.foreach{
      case (opId, statistics) =>
        if(!operatorRuntimeStateMap.contains(opId)){
          operatorRuntimeStateMap(opId) = new OperatorRuntimeState()
        }
        operatorRuntimeStateMap(opId).stats = statistics
    }
    onNext(WebWorkflowStatusUpdateEvent(evt))
  })

  controllerObservables.pythonPrintTriggered.subscribe((evt:PythonPrintTriggered) =>{
    operatorRuntimeStateMap(evt.operatorID).pythonMessages.add(evt.message)
    onNext(PythonPrintTriggeredEvent(evt))
  })

  controllerObservables.workflowExecutionError.subscribe((evt:ErrorOccurred) =>{
    workflowError = evt.error
    controller = null
    onNext(WorkflowExecutionErrorEvent(evt.error.getLocalizedMessage))
  })

  controllerObservables.workflowCompleted.subscribe((evt:WorkflowCompleted) =>{
    controller = null
  })

  override def sendSnapshotTo(observer: Observer[TexeraWebSocketEvent]): Unit = {
    observer.onNext(WebWorkflowStatusUpdateEvent(operatorRuntimeStateMap.map{case (opId, state) => (opId, state.stats)}.toMap))
    operatorRuntimeStateMap.foreach{
      case (opId, state) =>
        if(state.faults.nonEmpty) {
          observer.onNext(BreakpointTriggeredEvent(state.faults.toArray, opId))
        }
        if(!state.pythonMessages.isEmpty) {
          val stringBuilder = new StringBuilder()
          state.pythonMessages.forEach(s => stringBuilder.append(s))
          observer.onNext(PythonPrintTriggeredEvent(stringBuilder.toString(), opId))
        }
    }
    if(workflowError != null){
      observer.onNext(WorkflowExecutionErrorEvent(workflowError.getLocalizedMessage))
    }
  }
  def addBreakpoint(request: AddBreakpointRequest): Unit = {
    if(isActive){
      addBreakpoint(request.operatorID, request.breakpoint)
    }
  }

  def skipTuple(tupleReq: SkipTupleRequest): Unit = {
    throw new RuntimeException("skipping tuple is temporarily disabled")
  }

  def modifyLogic(operatorDescriptor: OperatorDescriptor): Unit = {
    if(isActive) {
      controller ! ControlInvocation(AsyncRPCClient.IgnoreReply, ModifyLogic(operatorDescriptor))
    }
  }

  def retryWorkflow(): Unit = {
    if(isActive) {
      clearTriggeredBreakpoints()
      controller ! ControlInvocation(AsyncRPCClient.IgnoreReply, RetryWorkflow())
      onNext(WorkflowResumedEvent())
    }
  }

  def pauseWorkflow(): Unit = {
    if(isActive) {
      controller ! ControlInvocation(AsyncRPCClient.IgnoreReply, PauseWorkflow())
    }
    // workflow paused event will be send after workflow is actually paused
    // the callback function will handle sending the paused event to frontend
  }

  def resumeWorkflow(): Unit = {
    if(isActive) {
      clearTriggeredBreakpoints()
      controller ! ControlInvocation(AsyncRPCClient.IgnoreReply, ResumeWorkflow())
      onNext(WorkflowResumedEvent()) // TODO: this should be sent after the workflow ACTUALLY resumes
    }
  }

  def killWorkflow(): Unit = {
    if(isActive) {
      controller ! PoisonPill
      logger.info("workflow killed")
    }
  }

  def removeBreakpoint(removeBreakpoint: RemoveBreakpointRequest): Unit = {
    throw new UnsupportedOperationException()
  }


}
