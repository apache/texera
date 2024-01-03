package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalLinkIdentity

object LinkWorkersHandler {
  final case class LinkWorkers(linkId: PhysicalLinkIdentity) extends ControlCommand[Unit]
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
      val partitionings = cp.workflow.physicalPlan.getLink(msg.linkId).partitionings
      val senderWorkers = cp.workflow.physicalPlan.getOperator(msg.linkId.from).identifiers
      val futures = senderWorkers
        .zip(partitionings)
        .flatMap({
          case (upstreamWorkerId, (partitioning, receiverWorkers)) =>
            Seq(
              send(AddPartitioning(msg.linkId, partitioning), upstreamWorkerId)
            ) ++ receiverWorkers.map(
              send(UpdateInputLinking(upstreamWorkerId, msg.linkId), _)
            )
        })

      Future.collect(futures.toSeq).map { _ =>
        // returns when all has completed

      }
    }
  }

}
