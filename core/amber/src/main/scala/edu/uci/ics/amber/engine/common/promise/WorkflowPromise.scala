package edu.uci.ics.amber.engine.common.promise

import com.twitter.util.Promise

object WorkflowPromise {
  def apply[T](ctx: PromiseContext): WorkflowPromise[T] = new WorkflowPromise[T](ctx)
}

/** This class represents the promise control message, which can be passed
  * across different actors to achieve the control logic that require the
  * participation of multiple actors.
  * @param ctx the creator and id pair of this promise.
  * @tparam T the return value type.
  */
class WorkflowPromise[T](val ctx: PromiseContext) extends Promise[T] {

  type returnType = T

  override def setValue(result: returnType): Unit = super.setValue(result)
}
