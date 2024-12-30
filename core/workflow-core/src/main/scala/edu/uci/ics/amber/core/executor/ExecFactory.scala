package edu.uci.ics.amber.core.executor

object ExecFactory {

  def newExecFromJavaCode(code: String): OperatorExecutor = {
    JavaRuntimeCompilation
      .compileCode(code)
      .getDeclaredConstructor()
      .newInstance()
      .asInstanceOf[OperatorExecutor]
  }

  def newExecFromJavaClassName[K](
      className: String,
      descString: String = "",
      idx: Int = 0,
      workerCount: Int = 1
  ): OperatorExecutor = {
    val clazz = Class.forName(className).asInstanceOf[Class[K]]
    try {
      if (descString.isEmpty) {
        clazz.getDeclaredConstructor().newInstance().asInstanceOf[OperatorExecutor]
      } else {
        clazz
          .getDeclaredConstructor(classOf[String])
          .newInstance(descString)
          .asInstanceOf[OperatorExecutor]
      }
    } catch {
      case e: NoSuchMethodException =>
        if (descString.isEmpty) {
          clazz
            .getDeclaredConstructor(classOf[Int], classOf[Int])
            .newInstance(idx, workerCount)
            .asInstanceOf[OperatorExecutor]
        } else {
          clazz
            .getDeclaredConstructor(classOf[String], classOf[Int], classOf[Int])
            .newInstance(descString, idx, workerCount)
            .asInstanceOf[OperatorExecutor]
        }
    }
  }
}
