package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddOutputPolicyHandler.AddOutputPolicy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ActorVirtualIdentityMessage
}

import java.io.FileOutputStream
import scala.util.Using

object LinkWorkersHandler {
  final case class LinkWorkers(link: LinkStrategy) extends ControlCommand[CommandCompleted]
}

/** add a data transfer policy to the sender workers and update input linking
  * for the receiver workers of a link strategy.
  *
  * possible sender: controller, client
  */
trait LinkWorkersHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: LinkWorkers, sender) =>
    {
      // get the list of (sender id, policy, set of receiver ids) from the link
      val futures = msg.link.getPolicies.flatMap {
        case (from, policy, tos) =>
          // send messages to sender worker and receiver workers

          Using(new FileOutputStream("actor.pb")) { output =>
            from.asMessage.writeTo(output)
          }

          Using(new FileOutputStream("link.pb")) { output =>
            println(msg.link.id.toByteString)
            msg.link.id.writeTo(output)
          }
          Seq(send(AddOutputPolicy(policy), from)) ++ tos.map(
            send(UpdateInputLinking(from, msg.link.id), _)
          )
      }
      Future.collect(futures.toSeq).map { x =>
        // returns when all has completed
        CommandCompleted()
      }
    }
  }

}
