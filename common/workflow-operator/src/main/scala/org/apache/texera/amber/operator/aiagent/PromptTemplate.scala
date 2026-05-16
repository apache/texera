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

package org.apache.texera.amber.operator.aiagent

import org.apache.texera.amber.core.tuple.Tuple

import java.util.regex.{Matcher, Pattern}

object PromptTemplate {
  private val PlaceholderPattern: Pattern =
    Pattern.compile("\\{\\{\\s*([A-Za-z_][A-Za-z0-9_\\-.]*)\\s*\\}\\}")

  def render(template: String, tuple: Tuple): String = {
    if (template == null) {
      return ""
    }

    val matcher = PlaceholderPattern.matcher(template)
    val rendered = new StringBuffer()
    while (matcher.find()) {
      val attributeName = matcher.group(1)
      if (!tuple.getSchema.containsAttribute(attributeName)) {
        throw new IllegalArgumentException(
          s"Prompt template references missing column: $attributeName"
        )
      }
      val value = Option(tuple.getField[Any](attributeName)).map(_.toString).getOrElse("")
      matcher.appendReplacement(rendered, Matcher.quoteReplacement(value))
    }
    matcher.appendTail(rendered)
    rendered.toString
  }
}
