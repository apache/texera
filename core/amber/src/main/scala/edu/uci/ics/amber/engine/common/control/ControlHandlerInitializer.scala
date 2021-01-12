package edu.uci.ics.amber.engine.common.control

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.control.ControlMessageReceiver.ControlCommand

import scala.reflect.ClassTag

/** class for developers to write control command handlers
  * usage:
  * 0. You need to know who is handling the control command and find its initializer
  *    i.e. if this control will be handled in worker, you will use WorkerControlHandlerInitializer class
  * 1. create a file of your control command handler -> "MyControlHandler.scala"
  * 2. create your own control command and identify its return type
  *    In the following example, the control command takes an int and returns an int:
  *    case class MyControl(param1:Int) extends ControlCommand[Int]
  * 3. create a handler and mark it's self-type as the initializer, then register your command:
  *    class MyControlHandler{
  *          this: WorkerControlHandlerInitializer =>
  *          registerHandler{
  *             mycmd:MyControl =>
  *               //do something
  *               val temp = 50
  *               //invoke another control command that returns an int
  *               send(OtherControl(), Others).map{
  *                 ret =>
  *                   ret + temp
  *               }
  *          }
  *
  * @param ctrlSource
  * @param ctrlReceiver
  */
class ControlHandlerInitializer(
    ctrlSource: ControlMessageSource,
    ctrlReceiver: ControlMessageReceiver
) {

  /** register a sync handler for one type of control command
    * @param handler the lambda function for handling that type of control, it returns B
    * @param ev enforce the compiler to check the input type of the handler extends control command
    *           also shows error on the editor when the return type is not correct
    * @tparam B the return type of the control command
    * @tparam C control command type
    */
  def registerHandler[B, C: ClassTag](
      handler: C => B
  )(implicit ev: C <:< ControlCommand[B]): Unit = {
    registerImpl({ case c: C => handler(c) })
  }

  /** register an async handler for one type of control command
    * @param handler the lambda function for handling that type of control, it returns future[B]
    * @param ev enforce the compiler to check the input type of the handler extends control command
    *           also shows error on the editor when the return type is not correct
    * @param d dummy param to prevent double definition of registerHandler
    * @tparam B the return type of the control command
    * @tparam C control command type
    */
  def registerHandler[B, C: ClassTag](
      handler: C => Future[B]
  )(implicit ev: C <:< ControlCommand[B], d: DummyImplicit): Unit = {
    registerImpl({ case c: C => handler(c) })
  }

  private def registerImpl(newHandler: PartialFunction[ControlCommand[_], Any]): Unit = {
    ctrlReceiver.registerHandler(newHandler)
  }

  def send[T](cmd: ControlCommand[T], to: ActorVirtualIdentity): Future[T] = {
    ctrlSource.send(cmd, to)
  }

}
