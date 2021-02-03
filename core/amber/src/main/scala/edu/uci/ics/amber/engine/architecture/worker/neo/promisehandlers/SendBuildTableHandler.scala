package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.AcceptBuildTableHandler.AcceptBuildTable
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.SendBuildTableHandler.SendBuildTable
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec

import scala.collection.mutable.ArrayBuffer

// join-skew research related.
object SendBuildTableHandler {
  final case class SendBuildTable(
      freeReceiverId: ActorVirtualIdentity
  ) extends ControlCommand[Seq[Unit]]
}

trait SendBuildTableHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { cmd: SendBuildTable =>
    // workerStateManager.shouldBe(Running, Ready)
    val buildMaps =
      dataProcessor.getOperatorExecutor().asInstanceOf[HashJoinOpExec[String]].getBuildHashTable()
    val buildSendingFutures = new ArrayBuffer[Future[Unit]]()
    buildMaps.foreach(map => {
      buildSendingFutures.append(send(AcceptBuildTable(map), cmd.freeReceiverId))
    })
    Future
      .collect(buildSendingFutures)
      .onSuccess(seq =>
        println(s"Replication of all parts of build table done to ${cmd.freeReceiverId}")
      )
  }
}
