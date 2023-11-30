package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.common.IOperatorExecutor

object OpExecInitInfo {

  type OpExecFunc = ((Int, OpExecConfig)) => IOperatorExecutor
  type JavaOpExecFunc = java.util.function.Function[(Int, OpExecConfig), IOperatorExecutor]
    with java.io.Serializable
  type OpExecInitInfo = ((Int, OpExecConfig)) => Either[IOperatorExecutor, String]

  def apply(code: String): OpExecInitInfo = _ => Right(code)
  def apply(opExecFunc: OpExecFunc): OpExecInitInfo = x => Left(opExecFunc(x))
  def apply(opExecFunc: JavaOpExecFunc): OpExecInitInfo = { x =>
    Left(opExecFunc.apply(x))
  }
}
