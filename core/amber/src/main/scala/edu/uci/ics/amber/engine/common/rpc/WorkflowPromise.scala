package edu.uci.ics.amber.engine.common.rpc

import com.twitter.util.Promise

object WorkflowPromise {
  def apply[T](): WorkflowPromise[T] = new WorkflowPromise[T]()
}

/** This class represents the promise control message, which can be passed
  * across different actors to achieve the control logic that require the
  * participation of multiple actors.
  * @tparam T the return value type.
  */
class WorkflowPromise[T]() extends Promise[T] {

  type returnType = T

  override def setValue(result: returnType): Unit = super.setValue(result)
}
