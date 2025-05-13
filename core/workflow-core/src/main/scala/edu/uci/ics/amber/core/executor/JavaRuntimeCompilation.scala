/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.amber.core.executor

import java.io.ByteArrayOutputStream
import java.net.URI
import java.util
import javax.tools._

object JavaRuntimeCompilation {
  val compiler: JavaCompiler = ToolProvider.getSystemJavaCompiler

  def compileCode(code: String): Class[_] = {
    val packageName = "edu.uci.ics.amber.operators.udf.java"

    //to hide it from user we will append the package in the udf code.
    val codeToCompile = s"package $packageName;\n$code"
    val defaultClassName = s"$packageName.JavaUDFOpExec"

    val fileManager: CustomJavaFileManager = new CustomJavaFileManager(
      compiler.getStandardFileManager(null, null, null)
    )

    // Diagnostic collector is to capture compilation diagnostics (errors, warnings, etc.)
    val diagnosticCollector = new DiagnosticCollector[JavaFileObject]

    /* Compiles the provided source code using the Java Compiler API, utilizing a custom file manager,
     Collecting compilation diagnostics, and storing the result in 'compilationResult'.
     */
    val compilationResult = compiler
      .getTask(
        null,
        fileManager,
        diagnosticCollector,
        null,
        null,
        util.Arrays.asList(new StringJavaFileObject(defaultClassName, codeToCompile))
      )
      .call()

    // Checking if compilation was successful
    if (!compilationResult) {
      // Getting the compilation diagnostics (errors and warnings)
      val diagnostics = diagnosticCollector.getDiagnostics
      val errorMessageBuilder = new StringBuilder()
      diagnostics.forEach { diagnostic =>
        errorMessageBuilder.append(
          s"Error at line ${diagnostic.getLineNumber}: ${diagnostic.getMessage(null)}\n"
        )
      }
      throw new RuntimeException(errorMessageBuilder.toString())
    }
    new CustomClassLoader().loadClass(defaultClassName, fileManager.getCompiledBytes)
  }

  private class CustomJavaFileManager(fileManager: JavaFileManager)
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

  private class StringJavaFileObject(className: String, code: String)
      extends SimpleJavaFileObject(
        URI.create(
          "string:///" + className.replace('.', '/') + JavaFileObject.Kind.SOURCE.extension
        ),
        JavaFileObject.Kind.SOURCE
      ) {
    override def getCharContent(ignoreEncodingErrors: Boolean): CharSequence = code
  }

  private class CustomClassLoader extends ClassLoader {

    def loadClass(name: String, classBytes: Array[Byte]): Class[_] =
      defineClass(name, classBytes, 0, classBytes.length)
  }
}
