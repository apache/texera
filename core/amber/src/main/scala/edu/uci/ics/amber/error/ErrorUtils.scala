package edu.uci.ics.amber.error

import com.google.protobuf.timestamp.Timestamp
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessage
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessageType.ERROR
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{ControlError, ErrorLanguage}
import edu.uci.ics.amber.util.VirtualIdentityUtils
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

import java.time.Instant
import scala.util.control.ControlThrowable

object ErrorUtils {

  /** A helper function for catching all throwable except some special scala internal throwable.
    * reference: https://www.sumologic.com/blog/why-you-should-never-catch-throwable-in-scala/
    *
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

  def mkConsoleMessage(actorId: ActorVirtualIdentity, err: Throwable): ConsoleMessage = {
    val source = if (err.getStackTrace.nonEmpty) {
      "(" + err.getStackTrace.head.getFileName + ":" + err.getStackTrace.head.getLineNumber + ")"
    } else {
      "(Unknown Source)"
    }
    val title = err.toString
    val message = err.getStackTrace.mkString("\n")
    ConsoleMessage(actorId.name, Timestamp(Instant.now), ERROR, source, title, message)
  }

  def mkControlError(err: Throwable): ControlError = {
    // Format each stack trace element with "at " prefix
    val stacktrace = err.getStackTrace.map(element => s"at ${element}").mkString("\n")
    if (err.getCause != null) {
      ControlError(err.toString, err.getCause.toString, stacktrace, ErrorLanguage.SCALA)
    } else {
      ControlError(err.toString, "", stacktrace, ErrorLanguage.SCALA)
    }
  }

  def reconstructThrowable(controlError: ControlError): Throwable = {
    if (controlError.language == ErrorLanguage.PYTHON) {
      return new Throwable(controlError.errorMessage)
    } else {
      val reconstructedThrowable = new Throwable(controlError.errorMessage)
      if (controlError.errorDetails.nonEmpty) {
        val causeThrowable = new Throwable(controlError.errorDetails)
        reconstructedThrowable.initCause(causeThrowable)
      }

      val stackTracePattern = """\s*at\s+(.+)\((.*)\)""".r
      val stackTraceElements = controlError.stackTrace.split("\n").flatMap { line =>
        line match {
          case stackTracePattern(className, location) =>
            Some(new StackTraceElement(className, "", location, -1))
          case _ => None
        }
      }
      reconstructedThrowable.setStackTrace(stackTraceElements)
      reconstructedThrowable
    }
  }

  def getStackTraceWithAllCauses(err: Throwable, topLevel: Boolean = true): String = {
    val header = if (topLevel) {
      "Stack trace for developers: \n\n"
    } else {
      "\n\nCaused by:\n"
    }
    val message = header + err.toString + "\n" + err.getStackTrace.mkString("\n")
    if (err.getCause != null) {
      message + getStackTraceWithAllCauses(err.getCause, topLevel = false)
    } else {
      message
    }
  }

  def getOperatorFromActorIdOpt(
      actorIdOpt: Option[ActorVirtualIdentity]
  ): (String, String) = {
    var operatorId = "unknown operator"
    var workerId = ""
    if (actorIdOpt.isDefined) {
      operatorId = VirtualIdentityUtils.getPhysicalOpId(actorIdOpt.get).logicalOpId.id
      workerId = actorIdOpt.get.name
    }
    (operatorId, workerId)
  }

}
