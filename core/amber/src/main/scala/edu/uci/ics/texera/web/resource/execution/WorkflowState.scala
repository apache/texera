package edu.uci.ics.texera.web.resource.execution

import java.time.LocalDateTime
import java.time.{Duration => JDuration}
import java.util.concurrent.ConcurrentHashMap

import akka.actor.Cancellable
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.web.TexeraWebApplication
import edu.uci.ics.texera.web.model.request.ExecuteWorkflowRequest
import edu.uci.ics.texera.web.resource.execution.WorkflowRuntimeService.{
  ExecutionStatusEnum,
  Running
}
import edu.uci.ics.texera.web.resource.execution.WorkflowState.WORKFLOW_CLEANUP_DEADLINE
import org.jooq.types.UInteger
import rx.lang.scala.Subscription

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
  var executionState: Option[WorkflowExecutionState] = None
  private var subCount = 0
  private var cleanUpJob: Cancellable = Cancellable.alreadyCancelled
  private var subscription: Subscription = Subscription()

  private[this] def setCleanUpDeadline(status: ExecutionStatusEnum): Unit = {
    synchronized {
      if (subCount > 0 || status == Running) {
        cleanUpJob.cancel()
        logger.info(
          s"[$wid] workflow state clean up postponed. current user count = $subCount, workflow status = $status"
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
      if (subCount > 0) {
        // do nothing
        logger.info(s"[$wid] workflow state clean up failed. current user count = $subCount")
      } else {
        WorkflowState.wIdToWorkflowState.remove(wid)
        executionState.foreach(_.workflowRuntimeService.killWorkflow())
        logger.info(s"[$wid] workflow state clean up completed.")
      }
    }
  }

  def connect(): Unit = {
    synchronized {
      subCount += 1
      cleanUpJob.cancel()
      logger.info(s"[$wid] workflow state clean up postponed. current user count = $subCount")
    }
  }

  def disconnect(): Unit = {
    synchronized {
      subCount -= 1
      if (
        subCount == 0 && !executionState.map(_.workflowRuntimeService.getStatus).contains(Running)
      ) {
        refreshDeadline()
      } else {
        logger.info(s"[$wid] workflow state clean up postponed. current user count = $subCount")
      }
    }
  }

  def initExecutionState(req: ExecuteWorkflowRequest, uidOpt: Option[UInteger]): Unit = {
    val prevResults = executionState match {
      case Some(value) => value.workflowResultService.operatorResults
      case None        => mutable.HashMap[String, OperatorResultService]()
    }
    val state = new WorkflowExecutionState(
      operatorCache,
      uidOpt,
      req,
      prevResults
    )
    subscription.unsubscribe()
    cleanUpJob.cancel()
    subscription = state.workflowRuntimeService.getStatusObservable.subscribe(status =>
      setCleanUpDeadline(status)
    )
    executionState = Some(state)
    state.startWorkflow()
  }
}
