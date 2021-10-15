package edu.uci.ics.texera.web.resource.execution

import java.time.LocalDateTime
import java.time.{Duration => JDuration}
import java.util.concurrent.ConcurrentHashMap

import akka.actor.Cancellable
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.web.TexeraWebApplication
import edu.uci.ics.texera.web.model.websocket.request.WorkflowExecuteRequest
import edu.uci.ics.texera.web.resource.execution.WorkflowRuntimeService.{
  ExecutionStatusEnum,
  Running
}
import edu.uci.ics.texera.web.resource.execution.WorkflowState.WORKFLOW_CLEANUP_DEADLINE
import org.jooq.types.UInteger
import rx.lang.scala.subjects.BehaviorSubject
import rx.lang.scala.{Observable, Subject, Subscription}

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object WorkflowState {
  private val wIdToWorkflowState = new ConcurrentHashMap[String, WorkflowState]()
  val WORKFLOW_CLEANUP_DEADLINE: FiniteDuration = 30.seconds

  def getOrCreate(wId: String): WorkflowState = {
    wIdToWorkflowState.compute(
      wId,
      (_, v) => {
        if (v == null) {
          new WorkflowState(wId)
        } else {
          v
        }
      }
    )
  }
}

class WorkflowState(wid: String) extends LazyLogging {
  // state across execution:
  val operatorCache: OperatorCache = new OperatorCache()
  var jobState: Option[WorkflowJobState] = None
  private var refCount = 0
  private var cleanUpJob: Cancellable = Cancellable.alreadyCancelled
  private var statusUpdateSubscription: Subscription = Subscription()
  private val jobStateSubject = BehaviorSubject[WorkflowJobState]()

  private[this] def setCleanUpDeadline(status: ExecutionStatusEnum): Unit = {
    synchronized {
      if (refCount > 0 || status == Running) {
        cleanUpJob.cancel()
        logger.info(
          s"[$wid] workflow state clean up postponed. current user count = $refCount, workflow status = $status"
        )
      } else {
        refreshDeadline()
      }
    }
  }

  private[this] def refreshDeadline(): Unit = {
    if (cleanUpJob.isCancelled || cleanUpJob.cancel()) {
      logger.info(
        s"[$wid] workflow state clean up will start at ${LocalDateTime.now().plus(JDuration.ofMillis(WORKFLOW_CLEANUP_DEADLINE.toMillis))}"
      )
      cleanUpJob = TexeraWebApplication.scheduleCallThroughActorSystem(WORKFLOW_CLEANUP_DEADLINE) {
        cleanUp()
      }
    }
  }

  private[this] def cleanUp(): Unit = {
    synchronized {
      if (refCount > 0) {
        // do nothing
        logger.info(s"[$wid] workflow state clean up failed. current user count = $refCount")
      } else {
        WorkflowState.wIdToWorkflowState.remove(wid)
        jobState.foreach(_.workflowRuntimeService.killWorkflow())
        logger.info(s"[$wid] workflow state clean up completed.")
      }
    }
  }

  def connect(): Unit = {
    synchronized {
      refCount += 1
      cleanUpJob.cancel()
      logger.info(s"[$wid] workflow state clean up postponed. current user count = $refCount")
    }
  }

  def disconnect(): Unit = {
    synchronized {
      refCount -= 1
      if (refCount == 0 && !jobState.map(_.workflowRuntimeService.getStatus).contains(Running)) {
        refreshDeadline()
      } else {
        logger.info(s"[$wid] workflow state clean up postponed. current user count = $refCount")
      }
    }
  }

  def initExecutionState(req: WorkflowExecuteRequest, uidOpt: Option[UInteger]): Unit = {
    val prevResults = jobState match {
      case Some(value) => value.workflowResultService.operatorResults
      case None        => mutable.HashMap[String, OperatorResultService]()
    }
    val state = new WorkflowJobState(
      operatorCache,
      uidOpt,
      req,
      prevResults
    )
    statusUpdateSubscription.unsubscribe()
    cleanUpJob.cancel()
    statusUpdateSubscription = state.workflowRuntimeService.getStatusObservable.subscribe(status =>
      setCleanUpDeadline(status)
    )
    jobState = Some(state)
    jobStateSubject.onNext(state)
    state.startWorkflow()
  }

  def getExecutionStateObservable: Observable[WorkflowJobState] = jobStateSubject
}
