package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.DataSendingPolicy
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddOutputPolicyHandler.AddOutputPolicy
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.Ready

object AddOutputPolicyHandler {
  final case class AddOutputPolicy(policy: DataSendingPolicy)
      extends ControlCommand[CommandCompleted]
}

trait AddOutputPolicyHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: AddOutputPolicy, sender) =>
    stateManager.assertState(Ready)
    tupleToBatchConverter.addPolicy(msg.policy)
    CommandCompleted()
  }

}
