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

package org.apache.texera.amber.operator.http.util

import org.apache.texera.amber.core.tuple.Tuple

import scala.util.matching.Regex

object TemplateInterpolator {
  private val placeholder: Regex = """\$\{([A-Za-z0-9_\.\- ]+)\}""".r

  // Replace ${fieldName} occurrences in `template` with the matching field's string value
  // from `tuple`. Unknown fields are replaced with an empty string.
  def interpolate(template: String, tuple: Tuple): String = {
    if (template == null) return ""
    placeholder.replaceAllIn(
      template,
      m => {
        val name = m.group(1)
        val value =
          try {
            val raw = tuple.getField[Any](name)
            if (raw == null) "" else raw.toString
          } catch {
            case _: Throwable => ""
          }
        Regex.quoteReplacement(value)
      }
    )
  }
}
