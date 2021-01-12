package edu.uci.ics.amber.engine.common.promise

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCServer.RPCCommand

class RPCHandlerInitializer(rpcClient: RPCClient, rpcServer: RPCServer) {

  def registerHandler(newHandler: PartialFunction[RPCCommand[_], Any]): Unit = {
    rpcServer.registerHandler(newHandler)
  }

  def send[T](cmd: RPCCommand[T], to: ActorVirtualIdentity): Future[T] = {
    rpcClient.send(cmd, to)
  }

}
