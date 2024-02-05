package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.Future
import edu.uci.ics.amber.engine.common.ambermessage.ChannelMarkerType
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  ChannelMarkerIdentity
}

import scala.language.implicitConversions
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

  /**
    * TODO: In scala 3, we can further simplify the command registration by using match types:
    * Thus we can write the following code:
    * trait Command[T]
    *
    * type ReturnType[X] = X match{
    *   case Command[t] => t
    * }
    * def foo[D < : Command[_](func: D => ReturnType[D]): Unit = {
    *   // Implementation...
    * }
    * case class I() extends Command[Int]
    * foo[I]((cmd) => {123}) // this will pass the compilation
    * foo[I]((cmd) => {"123"}) // this will fail
    */

  // syntax sugar to avoid explicit future conversion in control handlers.
  implicit def returnAsFuture[R](ret: R): Future[R] = Future[R](ret)

  /**
    * Registers a synchronous handler for a specific type of control command.
    * Note: This method allows multiple handlers for a control message and uses the most recently registered handler.
    *
    * @param handler a lambda function to handle the specified type of control command. It returns a value of type R.
    *
    * @tparam R the expected return type of the control command.
    * @tparam C the type of the control command that the handler will process.
    */
  def registerHandler[C <: ControlCommand[_]: ClassTag, R](
      handler: (C, ActorVirtualIdentity) => Future[R]
  )(implicit ev: C <:< ControlCommand[R]): Unit = {
    registerImpl({
      case (c: C, s) =>
        handler(c, s)
    })
  }

  private def registerImpl(
      newHandler: PartialFunction[(ControlCommand[_], ActorVirtualIdentity), Future[_]]
  ): Unit = {
    ctrlReceiver.registerHandler(newHandler)
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
