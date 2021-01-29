package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ActivateLinkHandler.ActivateLink
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddOutputPolicyHandler.AddOutputPolicy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object ActivateLinkHandler {
  final case class ActivateLink(link: LinkStrategy) extends ControlCommand[CommandCompleted]
}

trait ActivateLinkHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ActivateLink, sender) =>
    val futures = msg.link.getPolicies.flatMap {
      case (from, policy, tos) =>
        Seq(send(AddOutputPolicy(policy), from)) ++ tos.map(
          send(UpdateInputLinking(from, msg.link.id), _)
        )
    }
    Future.collect(futures.toSeq).map { x =>
      println("link activated!")
      CommandCompleted()
    }
  }

}
