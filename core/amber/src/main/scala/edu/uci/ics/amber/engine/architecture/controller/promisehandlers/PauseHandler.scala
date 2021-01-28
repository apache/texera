package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.{ControllerAsyncRPCHandlerInitializer, ControllerState}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{ReportCurrentProcessingTuple, WorkflowPaused}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.QueryCurrentInputTupleHandler.QueryCurrentInputTuple
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.model.event.WorkflowPausedEvent

import scala.collection.mutable

object PauseHandler{

  final case class PauseWorkflow() extends ControlCommand[CommandCompleted]
}


trait PauseHandler {
  this:ControllerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:PauseWorkflow, sender) =>
      val buffer = mutable.ArrayBuffer[(ITuple, ActorVirtualIdentity)]()
      Future.collect(workflow.getAllOperators.map {
        operator =>
          Future.collect(operator.getAllWorkers.map {
            worker =>
              send(PauseWorker(), worker).map {
                ret =>
                  send(QueryCurrentInputTuple(), worker).map {
                    tuple =>
                      buffer.append((tuple, worker))
                  }
              }
          }.toSeq).map{
            ret =>
              if(eventListener.reportCurrentTuplesListener != null){
                eventListener.reportCurrentTuplesListener.apply(ReportCurrentProcessingTuple(operator.tag.operator, buffer.toArray))
              }
          }
      }.toSeq).map{
        ret =>
          if(eventListener.workflowPausedListener != null){
            eventListener.workflowPausedListener.apply(WorkflowPaused())
          }
          actorContext.parent ! ControllerState.Paused // for testing
          CommandCompleted()
      }
  }

}
