package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ChannelMarkerType.NO_ALIGNMENT
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, ChannelMarkerType, PropagateChannelMarkerRequest, RetrieveWorkflowStateRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{RetrieveWorkflowStateResponse, StringResponse}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelMarkerIdentity}

import java.time.Instant
trait RetrieveWorkflowStateHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def sendRetrieveWorkflowState(request: RetrieveWorkflowStateRequest, ctx: AsyncRPCContext): Future[RetrieveWorkflowStateResponse] = {
    val markerMessage = PropagateChannelMarkerRequest(
      cp.workflowExecution.getRunningRegionExecutions
        .flatMap(_.getAllOperatorExecutions.map(_._1))
        .toSet,
      ChannelMarkerIdentity("RetrieveWorkflowState_" + Instant.now().toString),
      NO_ALIGNMENT,
      cp.workflowScheduler.physicalPlan,
      cp.workflowScheduler.physicalPlan.operators.map(_.id),
      RetrieveStateRequest()
    )
    controllerInterface.sendPropagateChannelMarker(
      markerMessage, mkContext(SELF)
    ).map { ret =>
      RetrieveWorkflowStateResponse(ret.returns.map {
        case (actorId, value) =>
          val finalret = value match{
            case s: StringResponse => s.value
            case other =>
              ""
          }
          (actorId, finalret)
      })
    }
  }

}
