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

import org.scalatest.flatspec.AnyFlatSpec

class AIAgentOutputParserSpec extends AnyFlatSpec {

  "AIAgentOutputParser.parseTextResult" should "extract a free-form text response" in {
    val response = AIAgentOutputParser.parseTextResult(
      """{"response":"useful summary"}""",
      List.empty
    )

    assert(response == "useful summary")
  }

  it should "validate text classification labels" in {
    val response = AIAgentOutputParser.parseTextResult(
      """{"response":"billing"}""",
      List("technical", "billing")
    )

    assert(response == "billing")
  }

  it should "fail when a text classification label is outside the allowed labels" in {
    val error = intercept[IllegalArgumentException] {
      AIAgentOutputParser.parseTextResult(
        """{"response":"sales"}""",
        List("technical", "billing")
      )
    }

    assert(error.getMessage.contains("not one of"))
  }

  "AIAgentOutputParser.parseStructured" should "extract configured JSON fields in order" in {
    val fields = AIAgentOutputParser.parseStructured(
      """{"sentiment":"positive","reason":"clear signal","score":0.91}""",
      List("sentiment", "reason", "score")
    )

    assert(fields == Seq("positive", "clear signal", "0.91"))
  }

  it should "return an empty string for missing or null structured fields" in {
    val fields = AIAgentOutputParser.parseStructured(
      """{"sentiment":null}""",
      List("sentiment", "reason")
    )

    assert(fields == Seq("", ""))
  }

  it should "validate structured classification field labels" in {
    val field = new AIAgentStructuredOutputField
    field.columnName = "sentiment"
    field.fieldType = AIAgentStructuredFieldType.Classification
    field.classificationLabels = List("positive", "negative")

    val fields = AIAgentOutputParser.parseStructuredFields(
      """{"sentiment":"positive"}""",
      List(field)
    )

    assert(fields == Seq("positive"))
  }

  it should "fail when a structured classification field label is outside the allowed labels" in {
    val field = new AIAgentStructuredOutputField
    field.columnName = "sentiment"
    field.fieldType = AIAgentStructuredFieldType.Classification
    field.classificationLabels = List("positive", "negative")

    val error = intercept[IllegalArgumentException] {
      AIAgentOutputParser.parseStructuredFields(
        """{"sentiment":"neutral"}""",
        List(field)
      )
    }

    assert(error.getMessage.contains("not one of"))
  }

  it should "fail when structured output is not a JSON object" in {
    val error = intercept[IllegalArgumentException] {
      AIAgentOutputParser.parseStructured("""["positive"]""", List("sentiment"))
    }

    assert(error.getMessage.contains("JSON object"))
  }

  "AIAgentOutputParser.parseClassification" should "extract a valid label and confidence" in {
    val (label, confidence) = AIAgentOutputParser.parseClassification(
      """{"label":"billing","confidence":0.82}""",
      List("technical", "billing")
    )

    assert(label == "billing")
    assert(confidence == 0.82)
  }

  it should "allow a missing confidence" in {
    val (label, confidence) = AIAgentOutputParser.parseClassification(
      """{"label":"technical"}""",
      List("technical", "billing")
    )

    assert(label == "technical")
    assert(confidence == null)
  }

  it should "fail when the label is outside the allowed labels" in {
    val error = intercept[IllegalArgumentException] {
      AIAgentOutputParser.parseClassification(
        """{"label":"sales","confidence":0.4}""",
        List("technical", "billing")
      )
    }

    assert(error.getMessage.contains("not one of"))
  }

  it should "fail when confidence is not numeric" in {
    val error = intercept[IllegalArgumentException] {
      AIAgentOutputParser.parseClassification(
        """{"label":"billing","confidence":"high"}""",
        List("technical", "billing")
      )
    }

    assert(error.getMessage.contains("confidence must be numeric"))
  }
}
