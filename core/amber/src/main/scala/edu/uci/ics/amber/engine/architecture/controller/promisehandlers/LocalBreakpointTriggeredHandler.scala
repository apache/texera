package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.BreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.AssignBreakpointHandler.AssignGlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LocalBreakpointTriggeredHandler.LocalBreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryAndRemoveBreakpointsHandler.QueryAndRemoveBreakpoints
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ResumeHandler.ResumeWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER

import scala.collection.mutable

object LocalBreakpointTriggeredHandler {
  final case class LocalBreakpointTriggered(localBreakpoints: Array[(String, Long)])
      extends ControlCommand[Unit]
}

/** indicate one/multiple local breakpoints have triggered on a worker
  * note that local breakpoints can only be triggered on outputted tuples,
  * if there is an error before the tuple gets outputted, a LocalOperatorException
  * message will be sent by the worker instead.
  *
  * possible sender: worker
  */
trait LocalBreakpointTriggeredHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: LocalBreakpointTriggered, sender) =>
    {
      // get the operator where the worker triggers breakpoint
      val targetOp = cp.executionState.getOperatorExecution(sender)
      val physicalOpId = cp.workflow.physicalPlan.getPhysicalOpByWorkerId(sender).id
      // get global breakpoints given local breakpoints
      val unResolved = msg.localBreakpoints
        .filter {
          case (id, ver) =>
            // validate the version of the breakpoint
            targetOp.attachedBreakpoints(id).hasSameVersion(ver)
        }
        .map(_._1)

      if (unResolved.isEmpty) {
        // no breakpoint needs to resolve, return directly
        Future {}
      } else {
        // we need to resolve global breakpoints
        // before query workers, increase the version number
        unResolved.foreach { bp =>
          targetOp.attachedBreakpoints(bp).increaseVersion()
        }
        // first pause the workers, then get their local breakpoints
        Future
          .collect(
            targetOp.getBuiltWorkerIds.map { worker =>
              send(PauseWorker(), worker).flatMap { ret =>
                send(QueryAndRemoveBreakpoints(unResolved), worker)
              }
            }.toSeq
          )
          .flatMap { bps =>
            // collect and handle breakpoints
            val collectedGlobalBreakpoints = bps.flatten
              .groupBy(_.id)
              .map {
                case (id, lbps) =>
                  val gbp = targetOp.attachedBreakpoints(id)
                  val localbps: Seq[gbp.localBreakpointType] =
                    lbps.map(_.asInstanceOf[gbp.localBreakpointType])
                  gbp.collect(localbps)
                  gbp
              }
            Future
              .collect(
                collectedGlobalBreakpoints
                  .filter(!_.isResolved)
                  .map { gbp =>
                    // attach new version if not resolved
                    execute(
                      AssignGlobalBreakpoint(gbp, physicalOpId.logicalOpId.id),
                      CONTROLLER
                    )
                  }
                  .toSeq
              )
              .flatMap { workersAssignedBreakpoint: Seq[List[ActorVirtualIdentity]] =>
                // report triggered breakpoints
                val triggeredBreakpoints = collectedGlobalBreakpoints.filter(_.isTriggered)
                if (triggeredBreakpoints.isEmpty) {
                  // if no triggered breakpoints, resume the workers of the current operator who has breakpoint assigned
                  Future
                    .collect(
                      workersAssignedBreakpoint.flatten
                        .toSet[ActorVirtualIdentity]
                        .map { worker => send(ResumeWorker(), worker) }
                        .toSeq
                    )
                    .unit
                } else {
                  // other wise, report to frontend and pause entire workflow
                  sendToClient(
                    BreakpointTriggered(mutable.HashMap.empty, physicalOpId.logicalOpId.id)
                  )
                  execute(PauseWorkflow(), CONTROLLER)
                }
              }
          }
      }
    }
  }
}
