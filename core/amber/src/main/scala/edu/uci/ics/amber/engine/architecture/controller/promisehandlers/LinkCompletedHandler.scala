package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkCompletedHandler.LinkCompleted
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}

object LinkCompletedHandler {
  final case class LinkCompleted(linkID: LinkIdentity) extends ControlCommand[Unit]
}

/** Notify the completion of a particular link
  * (the receiver side has received all data from one link,
  * note that this does not mean the receiver has completed
  * since there can be multiple input links)
  *
  * possible sender: worker
  */
trait LinkCompletedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: LinkCompleted, sender) =>
    {
      // get the target link from workflow
      val link = workflow.getLink(msg.linkID)
      link.incrementCompletedReceiversCount()
      if (link.isCompleted) {
        scheduler
          .onLinkCompletion(LinkIdentity(link.from.id, link.to.id))
          .flatMap(_ => Future.Unit)
      } else {
        // if the link is not completed yet, do nothing
        Future()
      }
    }
  }

}
