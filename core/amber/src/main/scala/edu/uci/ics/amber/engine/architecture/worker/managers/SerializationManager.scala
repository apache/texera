package edu.uci.ics.amber.engine.architecture.worker.managers

import edu.uci.ics.amber.core.executor._
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.InitializeExecutorRequest
import edu.uci.ics.amber.engine.common.{AmberLogging, CheckpointState, CheckpointSupport}
import edu.uci.ics.amber.util.VirtualIdentityUtils

class SerializationManager(val actorId: ActorVirtualIdentity) extends AmberLogging {

  @transient private var serializationCall: () => Unit = _
  private var execInitMsg: InitializeExecutorRequest = _

  def setOpInitialization(msg: InitializeExecutorRequest): Unit = {
    execInitMsg = msg
  }

  def restoreExecutorState(
      chkpt: CheckpointState
  ): (OperatorExecutor, Iterator[(TupleLike, Option[PortIdentity])]) = {
    val workerIdx = VirtualIdentityUtils.getWorkerIndex(actorId)
    val workerCount = execInitMsg.totalWorkerCount
    val executor = execInitMsg.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        ExecFactory.newExecFromJavaClassName(className, descString, workerIdx, workerCount)
      case OpExecWithCode(code, language) => ExecFactory.newExecFromJavaCode(code)
      case _                              => throw new UnsupportedOperationException("Unsupported OpExec type")
    }

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
