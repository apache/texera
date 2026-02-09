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

import scala.collection.mutable

private[amber] object PythonReflectionTextUtils {

  def indent(string: String, times: Int): String = {
    val pad = " " * times
    string.linesIterator.map(line => pad + line).mkString("\n")
  }

  def formatThrowable(throwable: Throwable): String = {
    val message = Option(throwable.getMessage).getOrElse("No message")
    val trace =
      throwable.getStackTrace.filter(_.getClassName.startsWith("org.apache.texera")).take(5)
    s"${throwable.getClass.getName}: $message\n${trace.mkString("\n")}"
  }

  def truncateBlock(string: String, maxLines: Int, maxChars: Int): String = {
    val lines = string.linesIterator.take(maxLines).toList
    val combined = lines.mkString("\n")
    if (combined.length > maxChars) combined.take(maxChars) + "..." else combined
  }

  def countOccurrences(targetHay: String, needle: String): Int = {
    if (needle.isEmpty) 0 else targetHay.split(java.util.regex.Pattern.quote(needle), -1).length - 1
  }

  def extractContexts(
      string: String,
      needle: String,
      radius: Int,
      maxContexts: Int
  ): Seq[String] = {
    val outArrayBuffer = mutable.ArrayBuffer.empty[String]
    var idx = string.indexOf(needle)
    while (idx != -1 && outArrayBuffer.size < maxContexts) {
      val start = math.max(0, idx - radius)
      val end = math.min(string.length, idx + needle.length + radius)
      outArrayBuffer += string.substring(start, end)
      idx = string.indexOf(needle, idx + 1)
    }
    outArrayBuffer.toSeq
  }
}
