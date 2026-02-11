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

import org.apache.texera.amber.pybuilder.PythonReflectionTextUtils.indent
import org.apache.texera.amber.pybuilder.PythonReflectionUtils.Finding

private[amber] object PythonRawTextReportRenderer {

  def render(findings: Seq[Finding], total: Int): String = {
    val grouped = findings.groupBy(_.kind)
    val stringBuilder = new StringBuilder
    stringBuilder.append(
      s"PythonRawTextReportRendererTest failures: ${findings.size} finding(s) across $total descriptor(s)\n"
    )

    def section(kind: String, title: String): Unit = {
      grouped.get(kind).foreach { items =>
        stringBuilder.append(s"\n== $title (${items.size}) ==\n")
        items.sortBy(_.clazz).foreach { f =>
          stringBuilder.append(s"- ${f.clazz}\n${indent(f.message.trim, 4)}\n")
        }
      }
    }

    section("instantiate", "Instantiation failures")
    section("injection-failure", "Injection failed")
    section("exception", "generatePythonCode exceptions")
    section("raw-invalid-text-leak", "Raw invalid text leaked into generated Python")
    section("py-compile", "py_compile failures")
    section("stdout", "Unexpected stdout")
    section("stderr", "Unexpected stderr")

    stringBuilder.toString()
  }
}
