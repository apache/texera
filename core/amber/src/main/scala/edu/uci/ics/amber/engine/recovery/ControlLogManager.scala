package edu.uci.ics.amber.engine.recovery

import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage

class ControlLogManager(
    logStorage: LogStorage[WorkflowControlMessage],
    controlInputPort: ControlInputPort
) extends RecoveryComponent {

  // For recovery, only need to replay control messages, and then it's done
  logStorage.load().foreach { msg => controlInputPort.handleControlMessage(msg) }
  setRecoveryCompleted()

  def persistControlMessage(msg: WorkflowControlMessage): Unit = {
    logStorage.persistElement(msg)
  }

}
