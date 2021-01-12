package edu.uci.ics.amber.engine.common.promise

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCServer.{
  AsyncRPCCommand,
  NormalRPCCommand,
  RPCCommand
}

class RPCHandlerInitializer(rpcClient: RPCClient, rpcServer: RPCServer) {

  def registerHandler[T](newHandler: PartialFunction[NormalRPCCommand[T], T]): Unit = {
    rpcServer.registerHandler(newHandler)
  }

  def registerHandlerAsync[T](newHandler: PartialFunction[AsyncRPCCommand[T], Future[T]]): Unit = {
    rpcServer.registerHandlerAsync(newHandler)
  }

  def send[T](cmd: RPCCommand[T], to: ActorVirtualIdentity): Future[T] = {
    rpcClient.send(cmd, to)
  }

}
