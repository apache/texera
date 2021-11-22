package edu.uci.ics.texera.web.service

import java.time.{LocalDateTime, Duration => JDuration}
import java.util.concurrent.ConcurrentHashMap

import akka.actor.Cancellable
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.web.TexeraWebApplication
import edu.uci.ics.texera.web.model.websocket.event.{ExecutionStatusEnum, Running}
import edu.uci.ics.texera.web.model.websocket.request.WorkflowExecuteRequest
import org.jooq.types.UInteger
import rx.lang.scala.subjects.BehaviorSubject
import rx.lang.scala.{Observable, Subscription}

import scala.collection.mutable
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object WorkflowService {
  private val wIdToWorkflowState = new ConcurrentHashMap[String, WorkflowService]()
  val cleanUpDeadlineInSeconds: Int =
    AmberUtils.amberConfig.getInt("web-server.workflow-state-cleanup-in-seconds")
  def getOrCreate(wId: String, cleanupTimeout: Int = cleanUpDeadlineInSeconds): WorkflowService = {
    wIdToWorkflowState.compute(
      wId,
      (_, v) => {
        if (v == null) {
          new WorkflowService(wId, cleanupTimeout)
        } else {
          v
        }
      }
    )
  }
}

class WorkflowService(wid: String, cleanUpTimeout: Int) extends LazyLogging {
  // state across execution:
  val operatorCache: WorkflowCacheService = new WorkflowCacheService()
  var jobService: Option[WorkflowJobService] = None
  private var refCount = 0
  private var cleanUpJob: Cancellable = Cancellable.alreadyCancelled
  private var statusUpdateSubscription: Subscription = Subscription()
  private val jobStateSubject = BehaviorSubject[WorkflowJobService]()

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
        s"[$wid] workflow state clean up will start at ${LocalDateTime.now().plus(JDuration.ofSeconds(cleanUpTimeout))}"
      )
      cleanUpJob = TexeraWebApplication.scheduleCallThroughActorSystem(cleanUpTimeout.seconds) {
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
        WorkflowService.wIdToWorkflowState.remove(wid)
        jobService.foreach(_.workflowRuntimeService.killWorkflow())
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
      if (refCount == 0 && !jobService.map(_.workflowRuntimeService.getStatus).contains(Running)) {
        refreshDeadline()
      } else {
        logger.info(s"[$wid] workflow state clean up postponed. current user count = $refCount")
      }
    }
  }

  def initExecutionState(req: WorkflowExecuteRequest, uidOpt: Option[UInteger]): Unit = {
    val prevResults = jobService match {
      case Some(value) => value.workflowResultService.operatorResults
      case None        => mutable.HashMap[String, OperatorResultService]()
    }
    val state = new WorkflowJobService(
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
    jobService = Some(state)
    jobStateSubject.onNext(state)
    state.startWorkflow()
  }

  def getJobServiceObservable: Observable[WorkflowJobService] = jobStateSubject.onTerminateDetach
}
