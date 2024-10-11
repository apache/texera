package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ReconfigureHandler.Reconfigure
import edu.uci.ics.amber.engine.common.model.PhysicalOp
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF
import edu.uci.ics.texera.web.service.FriesReconfigurationAlgorithm
import edu.uci.ics.texera.workflow.common.operators.StateTransferFunc

object ReconfigureHandler {

  final case class Reconfigure(
      reconfigurations: List[(PhysicalOp, Option[StateTransferFunc])],
      reconfigurationId: String
  ) extends ControlCommand[Unit]

}

trait ReconfigureHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler[Reconfigure, Unit] { (msg, sender) =>
    {
      val epochMarkers = FriesReconfigurationAlgorithm.scheduleReconfigurations(
        cp.workflowExecutionCoordinator,
        msg.reconfigurations,
        msg.reconfigurationId
      )
      epochMarkers.foreach(epoch => {
        send(epoch, SELF)
      })
    }
  }
}
