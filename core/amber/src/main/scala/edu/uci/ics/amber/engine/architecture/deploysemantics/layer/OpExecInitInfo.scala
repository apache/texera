package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig
import edu.uci.ics.amber.engine.common.IOperatorExecutor

object OpExecInitInfo {

  type OpExecFunc = (Int, PhysicalOp, OperatorConfig) => IOperatorExecutor
  type JavaOpExecFunc =
    java.util.function.Function[(Int, PhysicalOp, OperatorConfig), IOperatorExecutor]
      with java.io.Serializable

  def apply(code: String): OpExecInitInfo = OpExecInitInfoWithCode((_, _, _) => code)
  def apply(opExecFunc: OpExecFunc): OpExecInitInfo = OpExecInitInfoWithFunc(opExecFunc)
  def apply(opExecFunc: JavaOpExecFunc): OpExecInitInfo =
    OpExecInitInfoWithFunc((idx, physicalOp, operatorConfig) =>
      opExecFunc.apply(idx, physicalOp, operatorConfig)
    )
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

// only for Python right now
// TODO: add language type into this class
final case class OpExecInitInfoWithCode(codeGen: (Int, PhysicalOp, OperatorConfig) => String)
    extends OpExecInitInfo
final case class OpExecInitInfoWithFunc(
    opGen: (Int, PhysicalOp, OperatorConfig) => IOperatorExecutor
) extends OpExecInitInfo
