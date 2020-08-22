package Clustering

import Clustering.ClusterListener.GetAvailableNodeAddresses
import Engine.Common.Constants
import akka.actor.{Actor, ActorLogging}

class SingleNodeListener extends Actor with ActorLogging {
  Constants.defaultNumWorkers = 2
  override def receive: Receive = {
    case GetAvailableNodeAddresses => sender ! Array(context.self.path.address)
  }
}
