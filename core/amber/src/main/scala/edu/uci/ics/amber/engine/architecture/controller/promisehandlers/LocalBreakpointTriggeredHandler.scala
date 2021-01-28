package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.BreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalBreakpointTriggeredHandler.LocalBreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.QueryBreakpointsHandler.QueryBreakpoints
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LocalBreakpointTriggeredHandler{
  final case class LocalBreakpointTriggered(localBreakpoints: Array[(String, Long)]) extends ControlCommand[CommandCompleted]
}


trait LocalBreakpointTriggeredHandler{
  this:ControllerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:LocalBreakpointTriggered, sender) =>
      val targetOp = workflow.getOperator(sender)
      val opID = targetOp.tag.operator
      val unResolved = msg.localBreakpoints.filter{
        case (id, ver) =>
        targetOp.attachedBreakpoints(id).hasSameVersion(ver)
      }.map(_._1)

      if(unResolved.isEmpty){
        // no breakpoint needs to be resolve, return directly
        Future{CommandCompleted()}
      }else{
        unResolved.foreach{
          bp =>
            targetOp.attachedBreakpoints(bp).increaseVersion()
        }
        val map = mutable.HashMap[(ActorVirtualIdentity, FaultedTuple), ArrayBuffer[String]]()
        Future.collect(targetOp.getAllWorkers.map{
          worker =>
            send(PauseWorker(),worker).flatMap{
              ret =>
                send(QueryBreakpoints(unResolved), worker).map{
                  bp =>
                  val key =(worker,FaultedTuple(bp.currentTuple, 0))
                  map(key) = bp.breakpoints.filter(_.triggeredByCurrentTuple).map(_.breakpoint.toString).to[ArrayBuffer]
                  bp
                }
            }
        }.toSeq).flatMap{
          bps =>
            // handle breakpoints
            if (eventListener.breakpointTriggeredListener != null) {
              bps.flatMap(bp => bp.breakpoints).groupBy(_.breakpoint.id).foreach{
                case (id, lbps) =>
                  val gbp = targetOp.attachedBreakpoints(id)
                  val localbps:Seq[gbp.localBreakpointType] = lbps.map(_.breakpoint.asInstanceOf[gbp.localBreakpointType])
                  gbp.collect(localbps)
              }
              eventListener.breakpointTriggeredListener.apply(
                BreakpointTriggered(map, opID)
              )
            }
            send(PauseWorkflow(),VirtualIdentity.Controller)
        }
      }
  }
}
