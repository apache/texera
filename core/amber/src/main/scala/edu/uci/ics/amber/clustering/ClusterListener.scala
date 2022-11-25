package edu.uci.ics.amber.clustering

import akka.actor.{Actor, ActorLogging, Address, ExtendedActorSystem}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import com.twitter.util.{Await, Future}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.web.service.WorkflowService

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

object ClusterListener {
  final case class GetAvailableNodeAddresses()
}

class ClusterListener extends Actor with ActorLogging {

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
      log.info(s"received member event = $evt")
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

  private def updateClusterStatus(evt: MemberEvent): Unit = {
    evt match {
      case MemberRemoved(member, previousStatus) =>
        log.info("Cluster node " + member + " is down! Trigger recovery process.")
        val futures = new ArrayBuffer[Future[Any]]
        WorkflowService.getAllWorkflowService.foreach { workflow =>
          val jobService = workflow.jobService.getValue
          if (jobService != null && !jobService.workflow.isCompleted) {
            try {
              futures.append(jobService.client.notifyNodeFailure(member.address))
            }
          }
        }
        Await.all(futures: _*)
      case other => //skip
    }

    val addr = getAllAddressExcludingMaster
    Constants.currentWorkerNum = addr.size * Constants.numWorkerPerNode
    log.info(
      "---------Now we have " + addr.size + s" nodes in the cluster [current default #worker per operator=${Constants.currentWorkerNum}]---------"
    )

  }

}
