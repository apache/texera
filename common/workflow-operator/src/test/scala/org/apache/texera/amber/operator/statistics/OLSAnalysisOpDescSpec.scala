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

package org.apache.texera.amber.operator.statistics

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{LogicalOp, PythonOperatorDescriptor}
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OLSAnalysisOpDescSpec extends AnyFlatSpec with Matchers {
  private def config: ObjectNode =
    objectMapper
      .readTree(
        """{"operatorType":"OLSAnalysis","target":"y","predictors":["x"]}"""
      )
      .asInstanceOf[ObjectNode]

  private def descriptor(node: ObjectNode = config): PythonOperatorDescriptor =
    objectMapper.treeToValue(node, classOf[LogicalOp]).asInstanceOf[PythonOperatorDescriptor]

  private val input = Schema(
    List(
      new Attribute("y", AttributeType.DOUBLE),
      new Attribute("x", AttributeType.INTEGER),
      new Attribute("z", AttributeType.LONG),
      new Attribute("text", AttributeType.STRING),
      new Attribute("flag", AttributeType.BOOLEAN)
    )
  )
  private def schemas(
      d: PythonOperatorDescriptor,
      schema: Schema = input
  ): Map[PortIdentity, Schema] =
    Map(d.operatorInfo.inputPorts.head.id -> schema)

  "OLS Analysis" should "advertise a full-table single-input blocking operator" in {
    val d = descriptor()
    d.operatorInfo.userFriendlyName shouldBe "OLS Analysis"
    d.operatorInfo.inputPorts should have size 1
    d.operatorInfo.outputPorts should have size 1
    d.operatorInfo.outputPorts.head.blocking shouldBe true
    d.parallelizable() shouldBe false
  }

  it should "default to an intercept and complete-case omission" in {
    val json = objectMapper.valueToTree[ObjectNode](descriptor())
    json.get("includeIntercept").asBoolean() shouldBe true
    json.get("missingValues").asText() shouldBe "omit"
  }

  it should "round-trip ordered predictors and non-default choices" in {
    val node = config
    node.putArray("predictors").add("z").add("x")
    node.put("includeIntercept", false).put("missingValues", "error")
    val d = descriptor(node)
    val encoded = objectMapper.writeValueAsString(d)
    val restored = objectMapper.readValue(encoded, classOf[LogicalOp])
    objectMapper.writeValueAsString(restored) shouldBe encoded
    noException should be thrownBy d.getOutputSchemas(schemas(d))
  }

  private val doubleColumns = List(
    "estimate",
    "std_error",
    "t_statistic",
    "p_value",
    "ci_lower",
    "ci_upper",
    "residual_std_error",
    "r_squared",
    "adj_r_squared",
    "f_statistic",
    "f_p_value"
  )
  private val countColumns = List("n_input", "n_used", "n_omitted", "df_model", "df_residual")

  it should "declare typed coefficient and model statistics" in {
    val d = descriptor()
    val schema = d.getOutputSchemas(schemas(d))(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("term").getType shouldBe AttributeType.STRING
    schema.getAttribute("is_intercept").getType shouldBe AttributeType.BOOLEAN
    doubleColumns.foreach(n => schema.getAttribute(n).getType shouldBe AttributeType.DOUBLE)
    countColumns.foreach(n => schema.getAttribute(n).getType shouldBe AttributeType.LONG)
    schema.getAttributes should have size (2 + doubleColumns.size + countColumns.size)
  }

  private def rejects(name: String, change: ObjectNode => Unit, message: String): Unit = {
    it should s"reject $name before execution" in {
      val node = config
      change(node)
      val d = descriptor(node)
      intercept[IllegalArgumentException](d.getOutputSchemas(schemas(d))).getMessage should include(
        message
      )
      // Code generation must remain syntactically valid for half-filled/malformed
      // UI settings. Schema propagation rejects them before execution; generated
      // code must also refuse them if invoked without schema propagation.
      d.generatePythonCode() should include("raise ValueError(configuration_error)")
    }
  }
  rejects("an empty response", _.put("target", ""), "response")
  rejects("a null response", _.putNull("target"), "response")
  rejects("no predictors", _.putArray("predictors"), "predictor")
  rejects("null predictors", _.putNull("predictors"), "predictor")
  rejects("a null predictor", _.putArray("predictors").addNull(), "predictor")
  rejects("an empty predictor", _.putArray("predictors").add(""), "predictor")
  rejects("duplicate predictors", _.putArray("predictors").add("x").add("x"), "unique")
  rejects("the response as a predictor", _.putArray("predictors").add("y"), "response")
  rejects("an unknown missing policy", _.put("missingValues", "zero"), "missing")
  rejects("a null missing policy", _.putNull("missingValues"), "missing")
  rejects("a null intercept choice", _.putNull("includeIntercept"), "intercept")

  for (column <- List("absent", "text", "flag")) {
    it should s"reject a non-numeric or absent response/predictor '$column'" in {
      for (field <- List("target", "predictors")) {
        val node = config
        if (field == "target") node.put(field, column)
        else node.putArray(field).add(column)
        val d = descriptor(node)
        intercept[IllegalArgumentException](
          d.getOutputSchemas(schemas(d))
        ).getMessage should include(column)
      }
    }
  }

  it should "reject an absent input schema clearly" in {
    intercept[IllegalArgumentException](
      descriptor().getOutputSchemas(Map.empty)
    ).getMessage should include("input")
  }

  it should "expose numeric selectors, missing-value choices and UI defaults" in {
    val schema = OperatorMetadataGenerator.generateOperatorJsonSchema(descriptor().getClass)
    val properties = schema.get("properties")
    properties.get("includeIntercept").get("default").asBoolean() shouldBe true
    properties.get("missingValues").get("default").asText() shouldBe "omit"
    properties.get("missingValues").get("enum").toString shouldBe "[\"omit\",\"error\"]"
    properties.get("predictors").get("minItems").asInt() shouldBe 1
    properties.get("predictors").get("uniqueItems").asBoolean() shouldBe true
    schema.get("attributeTypeRules").get("target").get("enum").toString shouldBe
      "[\"integer\",\"long\",\"double\"]"
  }

  it should "generate a full-table OLS fit without a train/test split" in {
    val code = descriptor().generatePythonCode()
    code should include("from statsmodels.regression.linear_model import OLS")
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("OLS(y, x, missing=\"raise\", hasconst=intercept).fit(use_t=True)")
    code should not include "train_test_split"
  }

  it should "generate complete-case handling for only the selected columns" in {
    val code = descriptor().generatePythonCode()
    code should include("columns = [target] + predictors")
    code should include("complete = table[columns].dropna()")
    code should include("if config[\"missing\"] == \"error\" and n_omitted:")
    code should include(
      "raise ValueError(f\"OLS found {n_omitted} rows with missing selected values\")"
    )
  }

  it should "generate rank validation and null handling for undefined statistics" in {
    val code = descriptor().generatePythonCode()
    code should include("if np.linalg.matrix_rank(x) != x.shape[1]:")
    code should include("OLS design is rank-deficient")
    code should include("return float(value) if np.isfinite(value) else None")
  }
}
