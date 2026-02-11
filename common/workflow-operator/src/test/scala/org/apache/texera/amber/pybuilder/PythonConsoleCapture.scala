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

package org.apache.texera.amber.pybuilder

import org.apache.texera.amber.pybuilder.PythonReflectionUtils.Captured

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

private[amber] object PythonConsoleCapture {

  def captureOutErr[A](thunk: => A): Captured[A] = {
    val outByteArrayOutStream = new ByteArrayOutputStream()
    val errByteArrayOutStream = new ByteArrayOutputStream()
    val outPrintStream = new PrintStream(outByteArrayOutStream)
    val errorPrintStream = new PrintStream(errByteArrayOutStream)

    val value = Console.withOut(outPrintStream) { Console.withErr(errorPrintStream) { thunk } }
    outPrintStream.flush()
    errorPrintStream.flush()
    Captured(
      value,
      outByteArrayOutStream.toString(StandardCharsets.UTF_8.name()),
      errByteArrayOutStream.toString(StandardCharsets.UTF_8.name())
    )
  }
}
