package edu.uci.ics.amber.engine.common.promise

import com.twitter.util.Promise
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

import scala.language.experimental.macros
import scala.reflect.ClassTag

class PromiseHandlerInitializer(promiseManager: PromiseManager) extends LazyLogging {

  // empty promise handler.
  // default behavior: discard
  private var promiseHandler: PartialFunction[ControlCommand[_], Unit] = {
    case promise =>
      logger.info(s"discarding $promise")
  }

  def registerHandler(newPromiseHandler: PartialFunction[ControlCommand[_], Unit]): Unit = {
    promiseHandler = newPromiseHandler orElse promiseHandler
  }

  def schedule[T](cmd: ControlCommand[T], on: ActorVirtualIdentity): Promise[T] = {
    promiseManager.schedule(cmd, on)
  }

  def schedule[T: ClassTag](seq: (ControlCommand[T], ActorVirtualIdentity)*): Promise[Seq[T]] = {
    promiseManager.schedule(seq: _*)
  }

  def returning(value: Any): Unit = {
    promiseManager.returning(value)
  }

  def returning(): Unit = {
    promiseManager.returning()
  }

  def createPromise[T](): (WorkflowPromise[T], PromiseContext) = {
    promiseManager.createPromise()
  }

  def getPromiseHandlers: PartialFunction[ControlCommand[_], Unit] = promiseHandler

}
