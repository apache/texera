package edu.uci.ics.texera.workflow.operators.udf.scala

import java.io.ByteArrayOutputStream
import java.net.URI
import scala.tools.nsc.{Global, Settings}
import scala.reflect.internal.util.BatchSourceFile
import scala.tools.nsc.io.VirtualDirectory
import scala.reflect.internal.util.AbstractFileClassLoader
import scala.tools.nsc.reporters.ConsoleReporter

object ScalaRuntimeCompilation {
  def compileCode(code: String): Class[_] = {
    val packageName = "edu.uci.ics.texera.workflow.operators.udf.scala"

    // Prepend the package declaration to the user code
    val codeToCompile = s"package $packageName;\n$code"
    val defaultClassName = s"$packageName.ScalaUDFOpExec"

    // Scala compiler settings
    val settings = new Settings()
    settings.usejavacp.value = true // Use the current JVM classpath

    // Output virtual directory for compiled bytecode (in-memory)
    val virtualDirectory = new VirtualDirectory("(memory)", None)
    settings.outputDirs.setSingleOutput(virtualDirectory)

    // Capture compiler output in a ByteArrayOutputStream
    val outputStream = new ByteArrayOutputStream()
    val printWriter = new java.io.PrintWriter(outputStream)

    // Create a ConsoleReporter to handle compilation messages
    val reporter = new ConsoleReporter(settings, null, printWriter)

    // Initialize the Scala compiler with the provided settings and reporter
    val global = new Global(settings, reporter)
    val run = new global.Run

    // Prepare the source code as an in-memory file
    val sourceFile = new BatchSourceFile("(inline)", codeToCompile)

    // Compile the source file
    run.compileSources(List(sourceFile))

    // Check if there were any errors during compilation
    if (reporter.hasErrors) {
      // Flush the PrintWriter to make sure we capture all messages
      printWriter.flush()
      throw new RuntimeException("Compilation failed:\n" + outputStream.toString)
    }

    // If successful, load and return the compiled class from the virtual directory
    new CustomClassLoader(virtualDirectory).loadClass(defaultClassName)
  }

  private class CustomClassLoader(outputDir: VirtualDirectory) extends AbstractFileClassLoader(outputDir, this.getClass.getClassLoader) {
    // This class loader can load classes from the virtual in-memory directory
  }
}
