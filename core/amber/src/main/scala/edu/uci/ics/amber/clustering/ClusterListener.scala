package edu.uci.ics.amber.clustering

import akka.actor.{Actor, Address}
import akka.cluster.ClusterEvent._
import akka.cluster.Cluster
import com.twitter.util.{Await, Future}
import edu.uci.ics.amber.clustering.ClusterListener.numWorkerNodesInCluster
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.{AmberLogging, AmberUtils, Constants}
import edu.uci.ics.texera.web.SessionState
import edu.uci.ics.texera.web.model.websocket.response.ClusterStatusUpdateEvent
import edu.uci.ics.texera.web.service.{WorkflowJobService, WorkflowService}
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.FAILED
import edu.uci.ics.texera.web.storage.JobStateStore.updateWorkflowState

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
    case ClusterListener.GetAvailableNodeAddresses =>
      sender ! getAllAddressExcludingMaster.toArray
  }

  private def getAllAddressExcludingMaster: Iterable[Address] = {
    cluster.state.members
      .filter { member =>
        member.address != Constants.masterNodeAddr
      }
      .map(_.address)
  }

  private def forcefullyStop(jobService: WorkflowJobService, cause: Throwable): Unit = {
    jobService.client.shutdown()
    jobService.stateStore.statsStore.updateState(stats =>
      stats.withEndTimeStamp(System.currentTimeMillis())
    )
    jobService.stateStore.jobMetadataStore.updateState { jobInfo =>
      updateWorkflowState(FAILED, jobInfo).withError(cause.getLocalizedMessage)
    }
  }

  private def updateClusterStatus(evt: MemberEvent): Unit = {
    evt match {
      case MemberRemoved(member, status) =>
        logger.info("Cluster node " + member + " is down!")
        val futures = new ArrayBuffer[Future[Any]]
        WorkflowService.getAllWorkflowService.foreach { workflow =>
          val jobService = workflow.jobService.getValue
          if (jobService != null && !jobService.workflow.isCompleted) {
            if (AmberUtils.amberConfig.getBoolean("fault-tolerance.enable-determinant-logging")) {
              logger.info(
                s"Trigger recovery process for execution id = ${jobService.stateStore.jobMetadataStore.getState.eid}"
              )
              try {
                futures.append(jobService.client.notifyNodeFailure(member.address))
              } catch {
                case t: Throwable =>
                  logger.warn(
                    s"execution ${jobService.workflow.getWorkflowId()} cannot recover! forcing it to stop"
                  )
                  forcefullyStop(jobService, t)
              }
            } else {
              logger.info(
                s"Kill execution id = ${jobService.stateStore.jobMetadataStore.getState.eid}"
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

    Constants.currentWorkerNum = numWorkerNodesInCluster * Constants.numWorkerPerNode
    logger.info(
      "---------Now we have " + numWorkerNodesInCluster + s" nodes in the cluster [current default #worker per operator=${Constants.currentWorkerNum}]---------"
    )

  }

}
