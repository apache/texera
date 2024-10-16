package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, WorkflowReconfigureRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF
import edu.uci.ics.texera.web.service.FriesReconfigurationAlgorithm
import edu.uci.ics.texera.workflow.common.operators.StateTransferFunc

trait ReconfigureHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def reconfigureWorkflow(msg: WorkflowReconfigureRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    val epochMarkers = FriesReconfigurationAlgorithm.scheduleReconfigurations(
      cp.workflowExecutionCoordinator,
      msg.reconfiguration,
      msg.reconfigurationId
    )
    epochMarkers.foreach(epoch => {
      controllerInterface.propagateChannelMarker(epoch, SELF)
    })
    EmptyReturn()
  }


}
