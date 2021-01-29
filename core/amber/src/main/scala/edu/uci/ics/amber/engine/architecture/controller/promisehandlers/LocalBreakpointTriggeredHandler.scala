package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.BreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.AssignBreakpointHandler.AssignGlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalBreakpointTriggeredHandler.LocalBreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryAndRemoveBreakpointsHandler.QueryAndRemoveBreakpoints
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, VirtualIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object LocalBreakpointTriggeredHandler {
  final case class LocalBreakpointTriggered(localBreakpoints: Array[(String, Long)])
      extends ControlCommand[CommandCompleted]
}

trait LocalBreakpointTriggeredHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: LocalBreakpointTriggered, sender) =>
    val targetOp = workflow.getOperator(sender)
    val opID = targetOp.id.operator
    val unResolved = msg.localBreakpoints
      .filter {
        case (id, ver) =>
          targetOp.attachedBreakpoints(id).hasSameVersion(ver)
      }
      .map(_._1)

    if (unResolved.isEmpty) {
      // no breakpoint needs to be resolve, return directly
      Future { CommandCompleted() }
    } else {
      unResolved.foreach { bp =>
        targetOp.attachedBreakpoints(bp).increaseVersion()
      }
      Future
        .collect(targetOp.getAllWorkers.map { worker =>
          send(PauseWorker(), worker).flatMap { ret =>
            send(QueryAndRemoveBreakpoints(unResolved), worker)
          }
        }.toSeq)
        .flatMap { bps =>
          // collect and handle breakpoints
          val collectAndReassign = Future.collect(
            bps.flatten
              .groupBy(_.id)
              .map {
                case (id, lbps) =>
                  val gbp = targetOp.attachedBreakpoints(id)
                  val localbps: Seq[gbp.localBreakpointType] =
                    lbps.map(_.asInstanceOf[gbp.localBreakpointType])
                  gbp.collect(localbps)
                  // attach new version
                  execute(AssignGlobalBreakpoint(gbp, targetOp.id), ActorVirtualIdentity.Controller)
              }
              .toSeq
          )

          collectAndReassign.flatMap { ret =>
            // check if global breakpoint triggered
            if (targetOp.attachedBreakpoints.values.exists(_.isTriggered)) {
              // if triggered, pause the workflow
              if (eventListener.breakpointTriggeredListener != null) {
                eventListener.breakpointTriggeredListener.apply(
                  BreakpointTriggered(mutable.HashMap.empty, opID)
                )
              }
              execute(PauseWorkflow(), ActorVirtualIdentity.Controller)
            } else {
              // if not, resume the current operator
              Future
                .collect(targetOp.getAllWorkers.map { worker =>
                  send(ResumeWorker(), worker)
                }.toSeq)
                .map { ret =>
                  CommandCompleted()
                }
            }
          }
        }
    }
  }
}
