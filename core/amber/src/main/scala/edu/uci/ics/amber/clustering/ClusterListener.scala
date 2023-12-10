package edu.uci.ics.amber.clustering

import akka.actor.{Actor, Address}
import akka.cluster.ClusterEvent._
import akka.cluster.Cluster
import com.google.protobuf.timestamp.Timestamp
import com.twitter.util.{Await, Future}
import edu.uci.ics.amber.clustering.ClusterListener.numWorkerNodesInCluster
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.{AmberConfig, AmberLogging}
import edu.uci.ics.texera.web.SessionState
import edu.uci.ics.texera.web.model.websocket.response.ClusterStatusUpdateEvent
import edu.uci.ics.texera.web.service.{WorkflowJobService, WorkflowService}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{COMPLETED, FAILED}
import edu.uci.ics.texera.web.storage.JobStateStore.updateWorkflowState
import edu.uci.ics.texera.web.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowFatalError

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

object ClusterListener {
  final case class GetAvailableNodeAddresses()
  var numWorkerNodesInCluster = 0
}

class ClusterListener extends Actor with AmberLogging {

  val actorId: ActorVirtualIdentity = ActorVirtualIdentity("ClusterListener")
  val cluster: Cluster = Cluster(context.system)

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(
      self,
      initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent]
    )
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case evt: MemberEvent =>
      logger.info(s"received member event = $evt")
      updateClusterStatus(evt)
    case ClusterListener.GetAvailableNodeAddresses() =>
      sender ! getAllAddressExcludingMaster.toArray
    case other =>
      println(other)
  }

  private def getAllAddressExcludingMaster: Iterable[Address] = {
    cluster.state.members
      .filter { member =>
        member.address != AmberConfig.masterNodeAddr
      }
      .map(_.address)
  }

  private def forcefullyStop(jobService: WorkflowJobService, cause: Throwable): Unit = {
    jobService.client.shutdown()
    jobService.jobStateStore.statsStore.updateState(stats =>
      stats.withEndTimeStamp(System.currentTimeMillis())
    )
    jobService.jobStateStore.jobMetadataStore.updateState { jobInfo =>
      logger.error("forcefully stopping execution", cause)
      updateWorkflowState(FAILED, jobInfo).addFatalErrors(
        WorkflowFatalError(
          EXECUTION_FAILURE,
          Timestamp(Instant.now),
          cause.toString,
          cause.getStackTrace.mkString("\n"),
          "unknown operator"
        )
      )
    }
  }

  private def updateClusterStatus(evt: MemberEvent): Unit = {
    evt match {
      case MemberRemoved(member, status) =>
        logger.info("Cluster node " + member + " is down!")
        val futures = new ArrayBuffer[Future[Any]]
        WorkflowService.getAllWorkflowService.foreach { workflow =>
          val jobService = workflow.jobService.getValue
          if (
            jobService != null && jobService.jobStateStore.jobMetadataStore.getState.state != COMPLETED
          ) {
            if (AmberConfig.isFaultToleranceEnabled) {
              logger.info(
                s"Trigger recovery process for execution id = ${jobService.jobStateStore.jobMetadataStore.getState.eid}"
              )
              try {
                futures.append(jobService.client.notifyNodeFailure(member.address))
              } catch {
                case t: Throwable =>
                  logger.warn(
                    s"execution ${jobService.workflow.getWorkflowId} cannot recover! forcing it to stop"
                  )
                  forcefullyStop(jobService, t)
              }
            } else {
              logger.info(
                s"Kill execution id = ${jobService.jobStateStore.jobMetadataStore.getState.eid}"
              )
              forcefullyStop(jobService, new RuntimeException("fault tolerance is not enabled"))
            }
          }
        }
        Await.all(futures: _*)
      case other => //skip
    }

    val addr = getAllAddressExcludingMaster
    numWorkerNodesInCluster = addr.size
    SessionState.getAllSessionStates.foreach { state =>
      state.send(ClusterStatusUpdateEvent(numWorkerNodesInCluster))
    }

    logger.info(
      "---------Now we have " + numWorkerNodesInCluster + s" nodes in the cluster---------"
    )

  }

}
