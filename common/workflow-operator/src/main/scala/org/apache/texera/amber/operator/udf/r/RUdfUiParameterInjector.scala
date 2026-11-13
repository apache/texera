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

package org.apache.texera.amber.operator.udf.r

import org.apache.texera.amber.operator.udf.{UiUDFParameter, UiUdfParameterContract}

import java.nio.charset.StandardCharsets

/** Emits calls to the R plugin SDK; metadata policy is shared and runtime behavior stays in texera-rudf. */
object RUdfUiParameterInjector {
  // Encode names and values as UTF-8 bytes so user-entered text never becomes executable R syntax.
  private def rString(value: String): String = {
    val bytes = Option(value).getOrElse("").getBytes(StandardCharsets.UTF_8)
    s".texera_decode(c(${bytes.map(byte => f"0x${byte & 0xff}%02x").mkString(",")}))"
  }

  def inject(code: String, uiParameters: List[UiUDFParameter]): String = {
    val parameters = Option(uiParameters).getOrElse(Nil)
    UiUdfParameterContract.validate(parameters)
    val names = parameters.map(_.attribute.getName)
    val userCode = Option(code).getOrElse("")
    if (parameters.isEmpty) return userCode
    // Unlike Rscript's file reader, rpy2 parses the string directly and rejects CRLF input.
    val executableCode = userCode.replace("\r\n", "\n").replace('\r', '\n')
    val entries = parameters.map { parameter =>
      s"list(type=${rString(parameter.attribute.getType.getName)}, value=${rString(parameter.value)}, inputType=${rString(parameter.inputType)})"
    }
    val typeEntries = UiUdfParameterContract.typesByToken.toList.sortBy(_._1).map {
      case (token, attributeType) => s"$token=${rString(attributeType.getName)}"
    }
    val resourceEntries = UiUdfParameterContract.resourceInputTypes.toList.sortBy(_._1).map {
      case (token, value) => s"$token=${rString(value)}"
    }
    s"# Texera UI parameters: SDK v1\ntexera_ui_parameters({\n$executableCode\n}, parameters=setNames(list(${entries
      .mkString(",")}), c(${names.map(rString).mkString(",")})), attribute_types=list(${typeEntries
      .mkString(",")}), resources=list(${resourceEntries.mkString(",")}))"
  }
}
