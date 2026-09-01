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

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

trait StandaloneCodeGenerator {

  def generateStandaloneCode(): String

  /**
    * The file's own name, for a script that reads it from its own directory
    * rather than through Texera's resolved URI.
    *
    * Taken from the last path segment instead of by parsing the whole string as a
    * URI: the resolver percent-encodes the file-relative segments but leaves the
    * repository and version names as the user typed them, so a dataset version
    * called `v3 - with long text` makes `new URI` throw on the space and no code
    * is generated at all.
    */
  protected def sourceBasename(rawPath: String): String = {
    val segment = rawPath.split("/").lastOption.getOrElse("")
    // Percent-decoding only, matching what `URI.getPath` used to return here: form
    // decoding would also turn a literal `+` in a file name into a space.
    URLDecoder.decode(segment.replace("+", "%2B"), StandardCharsets.UTF_8)
  }

  def producesDataFrame(): Boolean = true

  /**
    * Definitions this operator's standalone code depends on, emitted once near
    * the top of the script rather than inline.
    *
    * The translator concatenates operator bodies into a single module, so an
    * operator needing a helper class has nowhere to put it that another operator
    * would not duplicate. Helpers returned here are collected across the whole
    * plan and deduplicated by their text, so two sampling operators in one
    * workflow yield one copy of the generator they share.
    */
  def standaloneHelpers(): Seq[String] = Seq.empty
}
