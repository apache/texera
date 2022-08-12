package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object LinkWorkersHandler {
  final case class LinkWorkers(link: LinkStrategy) extends ControlCommand[Unit]
}

/** add a data transfer partitioning to the sender workers and update input linking
  * for the receiver workers of a link strategy.
  *
  * possible sender: controller, client
  */
trait LinkWorkersHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: LinkWorkers, sender) =>
    {
      // get the list of (sender id, partitioning, set of receiver ids) from the link
      val futures = msg.link.getPartitioning.flatMap {
        case (from, link, partitioning, tos) =>
          // send messages to sender worker and receiver workers
          Seq(send(AddPartitioning(link, partitioning), from)) ++ tos.map(
            send(UpdateInputLinking(from, msg.link.id), _)
          )
      }

      Future.collect(futures.toSeq).map { _ =>
        // returns when all has completed

      }
    }
  }

}
