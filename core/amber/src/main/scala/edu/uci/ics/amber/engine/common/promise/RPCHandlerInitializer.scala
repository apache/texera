package edu.uci.ics.amber.engine.common.promise

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.RPCServer.RPCCommand

import scala.reflect.ClassTag

class RPCHandlerInitializer(rpcClient: RPCClient, rpcServer: RPCServer) {

  def registerHandler[B, C:ClassTag](handler: C => B)(implicit ev: C <:< RPCCommand[B]):Unit = {
    registerImpl({case c:C =>handler(c) })
  }

  def registerHandler[B, C:ClassTag](handler: C => Future[B])(implicit ev: C <:< RPCCommand[B], d: DummyImplicit):Unit = {
    registerImpl({case c:C =>handler(c) })
  }

  private def registerImpl(newHandler:PartialFunction[RPCCommand[_],Any]): Unit ={
    rpcServer.registerHandler(newHandler)
  }


  def send[T](cmd: RPCCommand[T], to: ActorVirtualIdentity): Future[T] = {
    rpcClient.send(cmd, to)
  }

}
