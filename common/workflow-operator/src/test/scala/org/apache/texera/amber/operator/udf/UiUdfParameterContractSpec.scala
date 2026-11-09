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

package org.apache.texera.amber.operator.udf

import org.apache.texera.amber.operator.udf.python.PythonUdfUiParameterInjector
import org.apache.texera.amber.operator.udf.r.RUdfUiParameterInjector
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class UiUdfParameterContractSpec extends AnyFlatSpec with Matchers {
  private val fixture =
    objectMapper.readTree(getClass.getResourceAsStream("/udf/ui-parameter-contract.json"))
  private val injectors = Seq(
    "python" -> ((parameters: List[UiUDFParameter]) =>
      PythonUdfUiParameterInjector
        .inject("class ProcessTupleOperator(UDFOperatorV2):\n    pass\n", parameters)
    ),
    "r" -> ((parameters: List[UiUDFParameter]) =>
      RUdfUiParameterInjector.inject("function() {}", parameters)
    )
  )

  for ((language, inject) <- injectors; example <- fixture.path("metadata").elements().asScala) {
    s"$language UI parameter metadata" should s"honor the shared ${example.path("id").asText()} contract" in {
      val parameters = Option
        .when(!example.path("parameters").isNull)(
          example
            .path("parameters")
            .elements()
            .asScala
            .map(node => objectMapper.treeToValue(node, classOf[UiUDFParameter]))
            .toList
        )
        .orNull
      if (example.has("error")) {
        intercept[RuntimeException](inject(parameters)).getMessage should include(
          example.path("error").asText()
        )
      } else noException should be thrownBy inject(parameters)
    }
  }
}
