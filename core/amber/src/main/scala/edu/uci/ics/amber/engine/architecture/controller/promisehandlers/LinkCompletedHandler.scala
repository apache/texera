package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkCompletedHandler.LinkCompleted
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}

object LinkCompletedHandler{
  final case class LinkCompleted(linkID:LinkIdentity) extends ControlCommand[CommandCompleted]
}


trait LinkCompletedHandler {
  this:ControllerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:LinkCompleted, sender) =>
      println(s"received link completed from $sender for ${msg.linkID}")
      val link = workflow.getLink(msg.linkID)
      link.receiveCompleted()
      if(link.isCompleted){
        val layerWithDependencies = workflow.getAllLayers.filter(l => !l.canStart && l.hasDependency(msg.linkID))
        layerWithDependencies.foreach{
          layer =>
            layer.resolveDependency(msg.linkID)
        }
        Future.collect(layerWithDependencies.filter(_.canStart).flatMap(l =>l.workers.keys).map(send(StartWorker(),_)).toSeq)
      }
      CommandCompleted()
  }

}
