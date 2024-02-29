package edu.uci.ics.texera.workflow.operators.udf.java

import java.io.ByteArrayOutputStream
import java.net.URI
import java.util
import javax.tools._

object JavaRuntimeCompilation {

  def compileCode(code: String): Class[_] = {
    val packageName =
      s"package edu.uci.ics.texera.workflow.operators.udf.java;\n" //to hide it from user we will append the package in the udf code.
    val codeToCompile = packageName + code
    val defaultClassName = s"edu.uci.ics.texera.workflow.operators.udf.java.JavaUDFOpExec"

    val compiler = ToolProvider.getSystemJavaCompiler
    val fileManager: CustomJavaFileManager = new CustomJavaFileManager(
      compiler.getStandardFileManager(null, null, null)
    )
    val compilationUnits =
      util.Arrays.asList(new StringJavaFileObject(defaultClassName, codeToCompile))
    val task = compiler.getTask(null, fileManager, null, null, null, compilationUnits)

    try {
      task.call()
      val compiledBytes: Array[Byte] = fileManager.getCompiledBytes
      val classLoader = new CustomClassLoader
      val compiledClass = classLoader.loadClass(defaultClassName, compiledBytes)
      compiledClass
    } catch {
      case exception: Exception =>
        println(s"Exception: $exception")
        val nullClass: Class[_] = null
        nullClass
    }
  }

  class CustomJavaFileManager(fileManager: JavaFileManager)
      extends ForwardingJavaFileManager[JavaFileManager](fileManager) {
    private val outputBuffer: ByteArrayOutputStream = new ByteArrayOutputStream()

    def getCompiledBytes: Array[Byte] = outputBuffer.toByteArray

    override def getJavaFileForOutput(
        location: JavaFileManager.Location,
        className: String,
        kind: JavaFileObject.Kind,
        sibling: FileObject
    ): JavaFileObject = {
      new SimpleJavaFileObject(URI.create(s"string:///$className${kind.extension}"), kind) {
        override def openOutputStream(): ByteArrayOutputStream = outputBuffer
      }
    }
  }

  class StringJavaFileObject(className: String, code: String)
      extends SimpleJavaFileObject(
        URI.create(
          "string:///" + className.replace('.', '/') + JavaFileObject.Kind.SOURCE.extension
        ),
        JavaFileObject.Kind.SOURCE
      ) {
    override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = code
  }

  class CustomClassLoader extends ClassLoader {
    def loadClass(name: String, classBytes: Array[Byte]): Class[_] =
      defineClass(name, classBytes, 0, classBytes.length)
  }
}
