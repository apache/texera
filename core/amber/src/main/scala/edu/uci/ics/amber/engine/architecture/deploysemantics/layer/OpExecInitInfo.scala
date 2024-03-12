package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.common.IOperatorExecutor

object OpExecInitInfo {

  type OpExecFunc = (Int, Int) => IOperatorExecutor
  type JavaOpExecFunc =
    java.util.function.Function[(Int, Int), IOperatorExecutor] with java.io.Serializable

  def apply(code: String, language: String): OpExecInitInfo =
    OpExecInitInfoWithCode((_, _) => (code, language))
  def apply(opExecFunc: OpExecFunc): OpExecInitInfo = OpExecInitInfoWithFunc(opExecFunc)
  def apply(opExecFunc: JavaOpExecFunc): OpExecInitInfo =
    OpExecInitInfoWithFunc((idx, totalWorkerCount) => opExecFunc.apply(idx, totalWorkerCount))
}

/**
  * Information regarding initializing an operator executor instance
  * it could be two cases:
  *   - OpExecInitInfoWithFunc:
  *       A function to create an operator executor instance, with parameters:
  *       1) the worker index, 2) the PhysicalOp;
  *   - OpExecInitInfoWithCode:
  *       A function returning the code string that to be compiled in a virtual machine.
  */
sealed trait OpExecInitInfo

final case class OpExecInitInfoWithCode(
    codeGen: (Int, Int) => (String, String)
) extends OpExecInitInfo
final case class OpExecInitInfoWithFunc(
    opGen: (Int, Int) => IOperatorExecutor
) extends OpExecInitInfo
