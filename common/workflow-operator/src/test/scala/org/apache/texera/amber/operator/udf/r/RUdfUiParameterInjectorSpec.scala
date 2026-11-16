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

import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.udf.UiUDFParameter
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RUdfUiParameterInjectorSpec extends AnyFlatSpec with Matchers {
  private def parameter(name: String, kind: AttributeType, value: String): UiUDFParameter = {
    val result = new UiUDFParameter
    result.attribute = new Attribute(name, kind)
    result.value = value
    result
  }

  "R UI parameters" should "preserve scripts with no saved parameters" in {
    RUdfUiParameterInjector.inject("function() {}", Nil) shouldBe "function() {}"
    RUdfUiParameterInjector.inject(null, null) shouldBe ""
  }

  it should "serialize and compile parameters for both R operators and APIs" in {
    for (source <- Seq(false, true); tuple <- Seq(false, true)) {
      val language = if (tuple) "r-tuple" else "r-table"
      val script =
        if (tuple) "coro::generator(function() { yield(list()) })"
        else "function() { data.frame() }"
      val json = s"""{"operatorType":"${if (source) "RUDFSource" else "RUDF"}","code":${objectMapper
        .writeValueAsString(
          script
        )},"useTupleAPI":$tuple,"uiParameters":[{"attribute":{"attributeName":"count","attributeType":"integer"},"value":"7"}]}"""
      val descriptor = objectMapper.readValue(json, classOf[LogicalOp])
      val restored = objectMapper.readTree(objectMapper.writeValueAsString(descriptor))
      restored.path("uiParameters").get(0).path("value").asText() shouldBe "7"
      descriptor.getPhysicalOp(WorkflowIdentity(1), ExecutionIdentity(1)).opExecInitInfo shouldBe
        OpExecWithCode(
          RUdfUiParameterInjector
            .inject(script, List(parameter("count", AttributeType.INTEGER, "7"))),
          language
        )
    }
  }

  it should "emit a versioned SDK call with encoded values and no runtime implementation" in {
    val code = RUdfUiParameterInjector.inject(
      "function() {}",
      List(parameter("café", AttributeType.STRING, "\"; stop('injected') #"))
    )
    code should startWith("# Texera UI parameters: SDK v1\ntexera_ui_parameters(")
    code should include("attribute_types=list(")
    code should not include "UiParameter <- function"
    code should not include "stop('injected')"
  }
}
