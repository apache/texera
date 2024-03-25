package edu.uci.ics.amber.clustering

import akka.actor.{Actor, Address}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import edu.uci.ics.amber.clustering.ClusterListener.numWorkerNodesInCluster
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.{AmberConfig, AmberLogging}
import edu.uci.ics.texera.web.SessionState
import edu.uci.ics.texera.web.model.websocket.response.ClusterStatusUpdateEvent

object ClusterListener {
  final case class FetchAllComputationNodeAddrs()
  var numWorkerNodesInCluster = 0
}

class ClusterListener extends Actor with AmberLogging {

  val actorId: ActorVirtualIdentity = ActorVirtualIdentity("ClusterListener")
  val cluster: Cluster = Cluster(context.system)

  /**
    *   subscribe to cluster changes, re-subscribe when restart
    */

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
      logger.debug(s"received member event = $evt")
      updateClusterStatus()
    case ClusterListener.FetchAllComputationNodeAddrs() =>
      sender() ! getComputationNodeAddrs.toArray
    case other =>
      logger.info(other.toString)
  }

  private def getComputationNodeAddrs: Iterable[Address] = {
    cluster.state.members.map(_.address).filterNot { _ == AmberConfig.masterNodeAddr }
  }
  private def updateClusterStatus(): Unit = {
    numWorkerNodesInCluster = getComputationNodeAddrs.size
    SessionState.getAllSessionStates.foreach { state =>
      state.send(ClusterStatusUpdateEvent(numWorkerNodesInCluster))
    }

    logger.info(
      "---------Now we have " + numWorkerNodesInCluster + s" nodes in the cluster---------"
    )

  }

}
