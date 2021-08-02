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
  val availableNodeAddresses = new mutable.HashSet[Address]()

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
    case MemberUp(member) =>
      if (
        context.system
          .asInstanceOf[ExtendedActorSystem]
          .provider
          .getDefaultAddress == member.address
      ) {
        if (Constants.masterNodeAddr.isDefined) {
          availableNodeAddresses.add(self.path.address)
          Constants.currentDataSetNum += Constants.dataVolumePerNode
          Constants.currentWorkerNum += Constants.numWorkerPerNode
        }
      } else {
        if (Constants.masterNodeAddr != member.address.host) {
          availableNodeAddresses.add(member.address)
          Constants.currentDataSetNum += Constants.dataVolumePerNode
          Constants.currentWorkerNum += Constants.numWorkerPerNode
        }
      }
      log.info(
        "---------Now we have " + availableNodeAddresses.size + " nodes in the cluster---------"
      )
      log.info(
        "currentDataSetNum: " + Constants.currentDataSetNum + " numWorkers: " + Constants.currentWorkerNum
      )
    case UnreachableMember(member) =>
      removeMember(member)
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      removeMember(member)
      log.info("Member is Removed: {} after {}", member.address, previousStatus)
    case _: MemberEvent                            => // ignore
    case ClusterListener.GetAvailableNodeAddresses => sender ! availableNodeAddresses.toArray
  }

  private def removeMember(member: Member): Unit = {
    if (
      context.system
        .asInstanceOf[ExtendedActorSystem]
        .provider
        .getDefaultAddress == member.address
    ) {
      if (Constants.masterNodeAddr.isDefined) {
        availableNodeAddresses.remove(self.path.address)
        Constants.currentDataSetNum -= Constants.dataVolumePerNode
        Constants.currentWorkerNum -= Constants.numWorkerPerNode
      }
    } else {
      if (Constants.masterNodeAddr != member.address.host) {
        availableNodeAddresses.remove(member.address)
        Constants.currentDataSetNum -= Constants.dataVolumePerNode
        Constants.currentWorkerNum -= Constants.numWorkerPerNode
      }
    }
  }
}
