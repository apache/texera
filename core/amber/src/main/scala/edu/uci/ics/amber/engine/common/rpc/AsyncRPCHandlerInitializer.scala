package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.Future
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelMarkerType}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  ChannelMarkerIdentity
}

import scala.reflect.ClassTag

/** class for developers to write control command handlers
  * usage:
  * 0. You need to know who is handling the control command and find its initializer
  *    i.e. if this control will be handled in worker, you will use WorkerControlHandlerInitializer class
  * 1. create a file of your control command handler -> "MyControlHandler.scala"
  * 2. create your own control command and identify its return type
  *    In the following example, the control command takes an int and returns an int:
  *    case class MyControl(param1:Int) extends ControlCommand[Int]
  * 3. create a handler and mark its self-type as the initializer, then register your command:
  *    class MyControlHandler{
  *          this: WorkerControlHandlerInitializer =>
  *          registerHandler{
  *             (mycmd:MyControl,sender) =>
  *               //do something
  *               val temp = mycmd.param1
  *               //invoke another control command that returns an int
  *               send(OtherControl(), Others).map{
  *                 ret =>
  *                   ret + mycmd.param1
  *               }
  *          }
  *
  * @param ctrlSource
  * @param ctrlReceiver
  */
class AsyncRPCHandlerInitializer(
    ctrlSource: AsyncRPCClient,
    ctrlReceiver: AsyncRPCServer
) {

  /** register a sync handler for one type of control command
    * note that register handler allows multiple handlers for a control message and uses the latest handler.
    * @param handler the lambda function for handling that type of control, it returns B
    * @param ev enforce the compiler to check the input type of the handler extends control command
    *           also shows error on the editor when the return type is not correct
    * @tparam B the return type of the control command
    * @tparam C control command type
    */
  def registerHandler[B, C: ClassTag](
      handler: (C, ActorVirtualIdentity) => B
  )(implicit ev: C <:< ControlCommand[B]): Unit = {
    registerImpl({ case (c: C, s) => Future { handler(c, s) } })
  }

  private def registerImpl(
      newHandler: PartialFunction[(ControlCommand[_], ActorVirtualIdentity), Future[_]]
  ): Unit = {
    ctrlReceiver.registerHandler(newHandler)
  }

  /** register an async handler for one type of control command
    * note that register handler allows multiple handlers for a control message and uses the latest handler.
    * @param handler the lambda function for handling that type of control, it returns future[B]
    * @param ev enforce the compiler to check the input type of the handler extends control command
    *           also shows error on the editor when the return type is not correct
    * @param d dummy param to prevent double definition of registerHandler
    * @tparam B the return type of the control command
    * @tparam C control command type
    */
  def registerHandler[B, C: ClassTag](
      handler: (C, ActorVirtualIdentity) => Future[B]
  )(implicit ev: C <:< ControlCommand[B], d: DummyImplicit): Unit = {
    registerImpl({ case (c: C, s) => handler(c, s) })
  }

  def send[T](cmd: ControlCommand[T], to: ActorVirtualIdentity): Future[T] = {
    ctrlSource.send(cmd, to)
  }

  def sendChannelMarker(
      markerId: ChannelMarkerIdentity,
      markerType: ChannelMarkerType,
      scope: Set[ChannelIdentity],
      cmdMapping: Map[ActorVirtualIdentity, ControlInvocation],
      to: ChannelIdentity
  ): Unit = {
    ctrlSource.sendChannelMarker(markerId, markerType, scope, cmdMapping, to)
  }

  def createInvocation[T](cmd: ControlCommand[T]): (ControlInvocation, Future[T]) =
    ctrlSource.createInvocation(cmd)

  def execute[T](cmd: ControlCommand[T], sender: ActorVirtualIdentity): Future[T] = {
    ctrlReceiver.execute((cmd, sender)).asInstanceOf[Future[T]]
  }

  def sendToClient(cmd: ControlCommand[_]): Unit = {
    ctrlSource.sendToClient(cmd)
  }

}
