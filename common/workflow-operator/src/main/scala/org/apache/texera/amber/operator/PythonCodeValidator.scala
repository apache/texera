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

package org.apache.texera.amber.operator

object PythonCodeValidator {

  def validateNoFileIO(code: String): Unit = {
    val fileIOPatterns = Seq(
      ("""(?<![.\w])open\s*\(""".r, "open() function"),
      ("""\.read\s*\(""".r, ".read() method"),
      ("""\.write\s*\(""".r, ".write() method"),
      ("""\.readline\s*\(""".r, ".readline() method"),
      ("""\.readlines\s*\(""".r, ".readlines() method"),
      ("""\.writelines\s*\(""".r, ".writelines() method"),
      ("""(?m)^(?:from\s+os\s+import|import\s+os)""".r, "os module import"),
      ("""(?m)^(?:from\s+shutil\s+import|import\s+shutil)""".r, "shutil module import"),
      ("""(?m)^(?:from\s+pathlib\s+import|import\s+pathlib)""".r, "pathlib module import"),
      ("""(?m)^(?:from\s+io\s+import|import\s+io)""".r, "io module import"),
      ("""(?m)^(?:from\s+subprocess\s+import|import\s+subprocess)""".r, "subprocess module import"),
      ("""(?m)^(?:from\s+glob\s+import|import\s+glob)""".r, "glob module import"),
      ("""Path\s*\(""".r, "Path() constructor")
    )

    val codeWithoutComments = code
      .replaceAll("""#.*$""", "")
      .replaceAll("""'''[\s\S]*?'''""", "\"\"")
      .replaceAll("\"\"\"[\\s\\S]*?\"\"\"", "\"\"")

    val detectedPatterns = fileIOPatterns.flatMap {
      case (pattern, description) =>
        if (pattern.findFirstIn(codeWithoutComments).isDefined) Some(description) else None
    }

    if (detectedPatterns.nonEmpty) {
      throw new RuntimeException(
        s"File I/O operations are not allowed in Python UDF. " +
          s"Detected: ${detectedPatterns.mkString(", ")}. " +
          s"Please remove file I/O code"
      )
    }
  }

  def validateNoPrint(code: String): Unit = {
    val printPatterns = Seq(
      ("""(?<![.\w])print\s*\(""".r, "print() function")
    )

    val codeWithoutComments = code
      .replaceAll("""#.*$""", "")
      .replaceAll("""'''[\s\S]*?'''""", "\"\"")
      .replaceAll("\"\"\"[\\s\\S]*?\"\"\"", "\"\"")

    val detectedPatterns = printPatterns.flatMap {
      case (pattern, description) =>
        if (pattern.findFirstIn(codeWithoutComments).isDefined) Some(description) else None
    }

    if (detectedPatterns.nonEmpty) {
      throw new RuntimeException(
        s"Print statements are not allowed in Python UDF. " +
          s"Detected: ${detectedPatterns.mkString(", ")}. " +
          s"Please remove print statements or use logging instead"
      )
    }
  }

  def generatePythonCodeForRaisingException(ex: Throwable): String = {
    s"#EXCEPTION DURING CODE GENERATION: ${ex.getMessage}"
  }
}
