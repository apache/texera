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

package org.apache.texera.amber.operator.visualization.histogram

import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64
import scala.jdk.CollectionConverters._

class HistogramChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: HistogramChartOpDesc = _

  before {
    opDesc = new HistogramChartOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  // A column name is carried either literally (plain chunks) or as the
  // base64 payload of a runtime decode site (encoded chunks).
  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  // The part of an assert message that names the offending field.
  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  it should "throw AssertionError naming the Value Column when value is left empty" in {
    val ex = intercept[AssertionError](opDesc.createPlotlyFigure())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("value")
  }

  it should "throw AssertionError with a non-blank message via generatePythonCode when value is empty" in {
    val ex = intercept[AssertionError](opDesc.generatePythonCode())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
  }

  it should "render the figure with the configured value column when value is set" in {
    opDesc.value = "hist_value_col"
    val plain = opDesc.createPlotlyFigure().plain
    assert(carries(plain, "hist_value_col"))
    plain should include("px.histogram")
  }

  it should "render optional color/separateBy/pattern columns when configured" in {
    opDesc.value = "hist_value_col"
    opDesc.color = "hist_color_col"
    opDesc.separateBy = "hist_facet_col"
    opDesc.pattern = "hist_pattern_col"
    opDesc.marginal = "rug"
    val plain = opDesc.createPlotlyFigure().plain
    assert(carries(plain, "hist_value_col"))
    assert(carries(plain, "hist_color_col"))
    assert(carries(plain, "hist_facet_col"))
    assert(carries(plain, "hist_pattern_col"))
  }

  private def marginalEnum: Seq[String] =
    OperatorMetadataGenerator
      .generateOperatorJsonSchema(classOf[HistogramChartOpDesc])
      .path("properties")
      .path("marginal")
      .path("enum")
      .elements()
      .asScala
      .map(_.asText())
      .toSeq

  // The property editor validates a stored value against this enum, so leaving the
  // empty value out would flag every workflow saved before the enum existed.
  "HistogramChartOpDesc.marginal" should "declare its values, empty included, as a schema enum" in {
    marginalEnum should contain theSameElementsAs Seq("", "rug", "box", "violin", "histogram")
  }

  it should "read a workflow saved with an empty Distribution Type and omit the argument" in {
    val saved = objectMapper
      .readValue(
        """{"operatorType":"Histogram","value":"hist_value_col","marginal":""}""",
        classOf[LogicalOp]
      )
      .asInstanceOf[HistogramChartOpDesc]
    saved.marginal shouldBe ""
    saved.createPlotlyFigure().plain should not include "marginal="
  }

  it should "default to no marginal plot on a freshly dropped operator" in {
    opDesc.marginal shouldBe ""
    opDesc.value = "hist_value_col"
    opDesc.createPlotlyFigure().plain should not include "marginal="
  }
}
