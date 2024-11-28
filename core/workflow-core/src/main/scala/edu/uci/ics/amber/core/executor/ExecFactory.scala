package edu.uci.ics.amber.core.executor

object ExecFactory {
  def newExecFromJavaCode(code: String): OperatorExecutor = {
    JavaRuntimeCompilation
      .compileCode(code)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[OperatorExecutor]
  }
  def newExecFromJavaClassName(className: String, descString: String): OperatorExecutor = {
    Class
      .forName(className)
      .getDeclaredConstructor(classOf[String])
      .newInstance(descString)
      .asInstanceOf[OperatorExecutor]
  }

}
