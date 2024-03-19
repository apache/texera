package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo.generateJavaOpExec
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.InitializeOperatorLogicHandler.InitializeOperatorLogic
import edu.uci.ics.amber.engine.common.{
  AmberLogging,
  CheckpointState,
  CheckpointSupport,
  IOperatorExecutor,
  VirtualIdentityUtils
}
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

class SerializationManager(val actorId: ActorVirtualIdentity) extends AmberLogging {

  @transient private var serializationCall: () => Unit = _
  private var opInitMsg: InitializeOperatorLogic = _
  def setOpInitialization(msg: InitializeOperatorLogic): Unit = {
    opInitMsg = msg
  }

  def restoreOperatorState(
      chkpt: CheckpointState
  ): (IOperatorExecutor, Iterator[(TupleLike, Option[PortIdentity])]) = {
    val operator = generateJavaOpExec(
      opInitMsg.opExecInitInfo,
      VirtualIdentityUtils.getWorkerIndex(actorId),
      opInitMsg.totalWorkerCount
    )
    val iter = operator match {
      case support: CheckpointSupport =>
        support.deserializeState(chkpt)
      case _ => Iterator.empty
    }
    (operator, iter)
  }

  def registerSerialization(call: () => Unit): Unit = {
    serializationCall = call
  }

  def applySerialization(): Unit = {
    if (serializationCall != null) {
      serializationCall()
      serializationCall = null
    }
  }

}
