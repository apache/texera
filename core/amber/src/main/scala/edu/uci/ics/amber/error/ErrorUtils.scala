package edu.uci.ics.amber.error

import scala.util.control.ControlThrowable

object ErrorUtils {

  /** A helper function for catching all throwable except some special scala internal throwable.
    * reference: https://www.sumologic.com/blog/why-you-should-never-catch-throwable-in-scala/
    * @param handler
    * @tparam T
    * @return
    */
  def safely[T](handler: PartialFunction[Throwable, T]): PartialFunction[Throwable, T] = {
    case ex: ControlThrowable => throw ex
    // case ex: OutOfMemoryError (Assorted other nasty exceptions you don't want to catch)
    // If it's an exception they handle, pass it on
    case ex: Throwable if handler.isDefinedAt(ex) => handler(ex)
    // If they didn't handle it, rethrow automatically
  }

}
