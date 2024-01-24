package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ChannelMarkerHandler.PropagateChannelMarker
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, ChannelMarkerType}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelMarkerIdentity,
  PhysicalOpIdentity
}
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

  registerHandler { (msg: PropagateChannelMarker, sender) =>
    {
      // step1: create separate control commands for each target actor.
      val inputSet = msg.targetOps.flatMap { target =>
        cp.executionState.getOperatorExecution(target).getBuiltWorkerIds.map { worker =>
          worker -> createInvocation(msg.markerCommand)
        }
      }
      // step 2: packing all control commands into one compound command.
      val cmdMapping: Map[ActorVirtualIdentity, ControlInvocation] = inputSet.map {
        case (workerId, (control, _)) => (workerId, control)
      }.toMap
      val futures: Set[Future[(ActorVirtualIdentity, _)]] = inputSet.map {
        case (workerId, (_, future)) => future.map(ret => (workerId, ret))
      }

      // step 3: convert scope DAG to channels.
      val channelScope = cp.executionState.builtChannels.filter(channelId => {
        msg.scope.operators
          .map(_.id)
          .contains(VirtualIdentityUtils.getPhysicalOpId(channelId.from)) &&
          msg.scope.operators.map(_.id).contains(VirtualIdentityUtils.getPhysicalOpId(channelId.to))
      })
      val controlChannels = msg.sourceOpToStartProp.flatMap { source =>
        cp.executionState.getOperatorExecution(source).getBuiltWorkerIds.flatMap { worker =>
          Array(
            ChannelID(CONTROLLER, worker, isControl = true),
            ChannelID(worker, CONTROLLER, isControl = true)
          )
        }
      }

      val finalScope = channelScope ++ controlChannels

      // step 4: start prop, send marker through control channel with the compound command from sources.
      msg.sourceOpToStartProp.foreach { source =>
        cp.executionState.getOperatorExecution(source).getBuiltWorkerIds.foreach { worker =>
          sendChannelMarker(
            msg.id,
            msg.markerType,
            finalScope.toSet,
            cmdMapping,
            ChannelID(actorId, worker, isControl = true)
          )
        }
      }

      // step 5: wait for the marker propagation.
      Future.collect(futures.toList).map { ret =>
        cp.logManager.markAsReplayDestination(msg.id)
        ret
      }
    }
  }
}
