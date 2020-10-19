package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.architecture.principal.PrincipalState
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.architecture.controller.ControllerState

object StateMessage {

  final case class EnforceStateCheck()

}
