package edu.uci.ics.amber.clustering

import akka.actor.{Actor, ActorLogging, Address, ExtendedActorSystem}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import edu.uci.ics.amber.engine.common.Constants

import scala.collection.mutable

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
      classOf[MemberEvent],
      classOf[UnreachableMember]
    )
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: Receive = {
    case evt: MemberEvent =>
      log.info(s"received member event = $evt")
      updateClusterStatus()
    case ClusterListener.GetAvailableNodeAddresses => sender ! getAllAddressExcludingMaster.toArray
  }

  private def getAllAddressExcludingMaster: Iterable[Address] = {
    cluster.state.members
      .filter { member =>
        member.address != Constants.masterNodeAddr
      }
      .map(_.address)
  }

  private def updateClusterStatus(): Unit = {
    val addr = getAllAddressExcludingMaster
    Constants.currentWorkerNum = addr.size * Constants.numWorkerPerNode
    log.info(
      "---------Now we have " + addr.size + s" nodes in the cluster [current default #worker per operator=${Constants.currentWorkerNum}]---------"
    )
  }

}
