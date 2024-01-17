package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkCompletedHandler.LinkCompleted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink

object LinkCompletedHandler {
  final case class LinkCompleted(link: PhysicalLink) extends ControlCommand[Unit]
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
      val link = cp.executionState.getLinkExecution(msg.link)
      link.incrementCompletedReceiversCount()
      if (link.isCompleted) {
        cp.workflowScheduler
          .onLinkCompletion(cp.workflow, cp.actorRefService, cp.actorService, msg.link)
          .flatMap(_ => Future.Unit)
      } else {
        // if the link is not completed yet, do nothing
        Future(())
      }
    }
  }

}
