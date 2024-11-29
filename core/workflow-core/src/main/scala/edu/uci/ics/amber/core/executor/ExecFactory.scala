package edu.uci.ics.amber.core.executor

object ExecFactory {
  def newExecFromJavaCode(code: String): OperatorExecutor = {
    JavaRuntimeCompilation
      .compileCode(code)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[OperatorExecutor]
  }
  def newExecFromJavaClassName(
      className: String,
      descString: String,
      idx: Int = 0,
      workerCount: Int = 1
  ): OperatorExecutor = {
    if (idx == 0 && workerCount == 1) {

      Class
        .forName(className)
        .getDeclaredConstructor(classOf[String])
        .newInstance(descString)
        .asInstanceOf[OperatorExecutor]
    } else {
      Class
        .forName(className)
        .getDeclaredConstructor(classOf[String], classOf[Int], classOf[Int])
        .newInstance(descString, idx, workerCount)
        .asInstanceOf[OperatorExecutor]
    }
  }

}
