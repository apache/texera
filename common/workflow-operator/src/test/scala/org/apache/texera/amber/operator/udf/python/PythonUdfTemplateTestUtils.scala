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

package org.apache.texera.amber.operator.udf.python

import com.fasterxml.jackson.annotation.JsonProperty

/**
  * Shared helpers for the Python UDF template specs: read a descriptor's default
  * code template and simulate a user uncommenting one of its classes.
  */
object PythonUdfTemplateTestUtils {

  /** The `defaultValue` of the descriptor's `code` property, i.e. the template. */
  def defaultCode(descriptorClass: Class[_]): String =
    descriptorClass
      .getDeclaredField("code")
      .getAnnotation(classOf[JsonProperty])
      .defaultValue()

  /** Lines that are neither blank nor comments. */
  def activeLines(code: String): List[String] =
    code.linesIterator.map(_.trim).filterNot(l => l.isEmpty || l.startsWith("#")).toList

  /** Uncomment the template lines from `# class <name>` up to the next commented class. */
  def uncommentClass(code: String, className: String): String = {
    var inBlock = false
    code.linesIterator
      .map { line =>
        if (line.startsWith("# class ")) inBlock = line.startsWith(s"# class $className")
        if (inBlock && line.startsWith("#")) line.stripPrefix("#").stripPrefix(" ") else line
      }
      .mkString("\n")
  }
}
