package edu.uci.ics.texera.web.resource.execution

import java.util.concurrent.ConcurrentHashMap

import akka.actor.Cancellable
import edu.uci.ics.texera.web.TexeraWebApplication
import edu.uci.ics.texera.web.model.request.ExecuteWorkflowRequest
import edu.uci.ics.texera.web.resource.execution.WorkflowRuntimeService.{ExecutionStatusEnum, Running}
import edu.uci.ics.texera.web.resource.execution.WorkflowState.WORKFLOW_CLEANUP_DEADLINE
import org.jooq.types.UInteger
import rx.lang.scala.Subscription

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object WorkflowState{
  private val wIdToWorkflowState = new ConcurrentHashMap[String, WorkflowState]()
  val WORKFLOW_CLEANUP_DEADLINE:FiniteDuration = 30.seconds

  def getOrCreate(wId:String):WorkflowState = {
    wIdToWorkflowState.compute(wId, (_,v) =>{
      if(v == null){
        new WorkflowState(wId)
      }else{
        v
      }
    })
  }
}

class WorkflowState(wid:String) {
  // state across execution:
  val operatorCache:OperatorCache = new OperatorCache()
  var executionState:Option[WorkflowExecutionState] = None
  private var subCount = 0
  private var cleanUpJob:Cancellable = Cancellable.alreadyCancelled
  private var subscription:Subscription = Subscription()

  private[this] def setCleanUpDeadline(status:ExecutionStatusEnum): Unit ={
    synchronized{
      if(subCount > 0 || status == Running){
        cleanUpJob.cancel()
        println(wid+": clean up job cancelled due to user usage or running status")
      }else{
        refreshDeadline()
      }
    }
  }

  private[this] def refreshDeadline(): Unit ={
    if(cleanUpJob.isCancelled || cleanUpJob.cancel()){
      println(wid+": clean up job will begin in 30 seconds from now")
      cleanUpJob = TexeraWebApplication.scheduleCallThroughActorSystem(WORKFLOW_CLEANUP_DEADLINE){
        cleanUp()
      }
    }
  }

  private[this] def cleanUp(): Unit ={
    synchronized{
      if(subCount > 0){
        // do nothing
        println(wid+": Cannot clean up, at least one user is using this workflow state")
      }else{
        WorkflowState.wIdToWorkflowState.remove(wid)
        executionState.foreach(_.workflowRuntimeService.killWorkflow())
        println(wid+": clean up this workflow state")
      }
    }
  }

  def connect(): Unit ={
    synchronized {
      subCount += 1
      cleanUpJob.cancel()
      println(wid+": clean up job cancelled due to user usage")
    }
  }

  def disconnect():Unit = {
    synchronized{
      subCount -= 1
      if(subCount == 0 && !executionState.map(_.workflowRuntimeService.getStatus).contains(Running)){
        refreshDeadline()
      }
    }
  }

  def initExecutionState(req:ExecuteWorkflowRequest, uidOpt:Option[UInteger]): Unit = {
    val prevResults = executionState match {
      case Some(value) => value.workflowResultService.operatorResults
      case None => mutable.HashMap[String, OperatorResultService]()
    }
    val state = new WorkflowExecutionState(
      operatorCache,
      uidOpt,
      req,
      prevResults
    )
    subscription.unsubscribe()
    cleanUpJob.cancel()
    subscription = state.workflowRuntimeService.getStatusObservable.subscribe(status => setCleanUpDeadline(status))
    executionState = Some(state)
    state.startWorkflow()
  }
}
