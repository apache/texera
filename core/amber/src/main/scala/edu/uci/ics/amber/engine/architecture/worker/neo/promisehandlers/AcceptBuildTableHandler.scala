package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.AcceptBuildTableHandler.AcceptBuildTable
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// join-skew research related.
object AcceptBuildTableHandler {
  final case class AcceptBuildTable(
      buildHashMap: mutable.HashMap[String, ArrayBuffer[Tuple]]
  ) extends ControlCommand[Unit]
}

trait AcceptBuildTableHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { cmd: AcceptBuildTable =>
    // workerStateManager.shouldBe(Running, Ready)
    try {
      dataProcessor
        .getOperatorExecutor()
        .asInstanceOf[HashJoinOpExec[String]]
        .addToHashTable(cmd.buildHashMap)
    } catch {
      case exception: Exception =>
        println(
          "Exception happened" + exception.getMessage() + " stacktrace " + exception.getStackTrace()
        )
    }
  }
}
