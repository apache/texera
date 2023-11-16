package edu.uci.ics.amber.clustering

import edu.uci.ics.amber.clustering.ClusterListener.GetAvailableNodeAddresses
import edu.uci.ics.amber.engine.common.Constants
import akka.actor.{Actor, ActorLogging}

class SingleNodeListener extends Actor with ActorLogging {
  Constants.currentWorkerNum = 2
  override def receive: Receive = {
    case GetAvailableNodeAddresses() => sender ! Array(context.self.path.address)
  }
}
