package edu.uci.ics.amber.clustering

import edu.uci.ics.amber.clustering.ClusterListener.FetchAllComputationNodeAddrs
import akka.actor.{Actor, ActorLogging}

class SingleNodeListener extends Actor with ActorLogging {
  override def receive: Receive = {
    case FetchAllComputationNodeAddrs() => sender() ! Array(context.self.path.address)
  }
}
