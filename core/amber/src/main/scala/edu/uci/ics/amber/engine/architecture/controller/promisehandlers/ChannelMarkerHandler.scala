package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ChannelMarkerHandler.PropagateChannelMarker
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.ControllerInitiateQueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.ambermessage.ChannelMarkerType
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity, ChannelMarkerIdentity, PhysicalOpIdentity}
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

object ChannelMarkerHandler {

  final case class PropagateChannelMarker(
      sourceOpToStartProp: Set[PhysicalOpIdentity],
      id: ChannelMarkerIdentity,
      markerType: ChannelMarkerType,
      scope: PhysicalPlan,
      targetOps: Set[PhysicalOpIdentity],
      markerCommand: ControlCommand[_]
  ) extends ControlCommand[Seq[(ActorVirtualIdentity, _)]]

}

trait ChannelMarkerHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler[PropagateChannelMarker, Seq[(ActorVirtualIdentity, _)]] { (msg, sender) =>
    {
      // step1: create separate control commands for each target actor.
      val inputSet = msg.targetOps.flatMap { target =>
        cp.workflowExecution.getRunningRegionExecutions
          .map(_.getOperatorExecution(target))
          .flatMap(_.getWorkerIds.map { worker =>
            worker -> createInvocation(msg.markerCommand)
          })
      }
      // step 2: packing all control commands into one compound command.
      val cmdMapping: Map[ActorVirtualIdentity, ControlInvocation] = inputSet.map {
        case (workerId, (control, _)) => (workerId, control)
      }.toMap
      val futures: Set[Future[(ActorVirtualIdentity, _)]] = inputSet.map {
        case (workerId, (_, future)) => future.map(ret => (workerId, ret))
      }

      // step 3: convert scope DAG to channels.
      val channelScope = cp.workflowExecution.getRunningRegionExecutions
        .flatMap(regionExecution =>
          regionExecution.getAllLinkExecutions
            .map(_._2)
            .flatMap(linkExecution => linkExecution.getAllChannelExecutions.map(_._1))
        )
        .filter(channelId => {
          msg.scope.operators
            .map(_.id)
            .contains(VirtualIdentityUtils.getPhysicalOpId(channelId.fromWorkerId)) &&
            msg.scope.operators
              .map(_.id)
              .contains(VirtualIdentityUtils.getPhysicalOpId(channelId.toWorkerId))
        })
      val controlChannels = msg.sourceOpToStartProp.flatMap { source =>
        cp.workflowExecution.getLatestOperatorExecution(source).getWorkerIds.flatMap { worker =>
          Seq(
            ChannelIdentity(CONTROLLER, worker, isControl = true),
            ChannelIdentity(worker, CONTROLLER, isControl = true)
          )
        }
      }

      val finalScope = channelScope ++ controlChannels

      // step 4: start prop, send marker through control channel with the compound command from sources.
      msg.sourceOpToStartProp.foreach { source =>
        cp.workflowExecution.getLatestOperatorExecution(source).getWorkerIds.foreach { worker =>
          sendChannelMarker(
            msg.id,
            msg.markerType,
            finalScope.toSet,
            cmdMapping,
            ChannelIdentity(actorId, worker, isControl = true)
          )
        }
      }
      // step 5: wait for the marker propagation.
      Future.collect(futures.toList).map { ret =>
        if(!msg.markerCommand.isInstanceOf[PauseWorker]){
          cp.logManager.markAsReplayDestination(msg.id)
        }
        execute(ControllerInitiateQueryStatistics(None), CONTROLLER)
        ret
      }
    }
  }
}
