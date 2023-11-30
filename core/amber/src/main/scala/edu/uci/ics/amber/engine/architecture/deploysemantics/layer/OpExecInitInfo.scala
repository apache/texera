package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.common.IOperatorExecutor

import scala.compat.java8.FunctionConverters.enrichAsScalaFromFunction

object OpExecInitInfo {

  type OpExecFunc = Function[(Int, OpExecConfig), IOperatorExecutor]
  type JavaOpExecFunc = java.util.function.Function[(Int, OpExecConfig), IOperatorExecutor]
  type OpExecInitInfo = Either[OpExecFunc, String]

  def apply(code: String): OpExecInitInfo = Right(code)
  def apply(opExecFunc: OpExecFunc): OpExecInitInfo = Left(opExecFunc)
  def apply(opExecFunc: JavaOpExecFunc): OpExecInitInfo = Left(opExecFunc.asScala)
}
