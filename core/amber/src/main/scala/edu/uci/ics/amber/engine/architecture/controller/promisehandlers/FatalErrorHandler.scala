package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

object FatalErrorHandler {
  final case class FatalError(e: Throwable, fromActor: Option[ActorVirtualIdentity] = None)
      extends Throwable
      with ControlCommand[Unit]

  def getOperatorAndWorkerInfoFromError(
      actorIdOpt: Option[ActorVirtualIdentity]
  ): (String, String) = {
    var operatorId = "unknown operator"
    var workerId = ""
    if (actorIdOpt.isDefined) {
      operatorId = VirtualIdentityUtils.getPhysicalOpId(actorIdOpt.get).logicalOpId.id
      workerId = actorIdOpt.get.name
    }
    (operatorId, workerId)
  }
}
