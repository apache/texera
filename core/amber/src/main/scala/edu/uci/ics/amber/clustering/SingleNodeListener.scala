package edu.uci.ics.amber.clustering

import akka.actor.{Actor, ActorLogging}
import edu.uci.ics.amber.clustering.ClusterListener.GetAvailableNodeAddresses

class SingleNodeListener extends Actor with ActorLogging {
  override def receive: Receive = {
    case GetAvailableNodeAddresses() => sender() ! Array(context.self.path.address)
  }
}
