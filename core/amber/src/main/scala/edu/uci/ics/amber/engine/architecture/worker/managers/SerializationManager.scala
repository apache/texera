package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo.generateJavaOpExec
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.InitializeExecutorHandler.InitializeExecutor
import edu.uci.ics.amber.engine.common.{
  AmberLogging,
  CheckpointState,
  CheckpointSupport,
  VirtualIdentityUtils
}
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor

class SerializationManager(val actorId: ActorVirtualIdentity) extends AmberLogging {

  @transient private var serializationCall: () => Unit = _
  private var execInitMsg: InitializeExecutor = _
  def setOpInitialization(msg: InitializeExecutor): Unit = {
    execInitMsg = msg
  }

  def restoreExecutorState(
      chkpt: CheckpointState
  ): (OperatorExecutor, Iterator[(TupleLike, Option[PortIdentity])]) = {
    val executor = generateJavaOpExec(
      execInitMsg.opExecInitInfo,
      VirtualIdentityUtils.getWorkerIndex(actorId),
      execInitMsg.totalWorkerCount
    )
    val iter = executor match {
      case support: CheckpointSupport =>
        support.deserializeState(chkpt)
      case _ => Iterator.empty
    }
    (executor, iter)
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
