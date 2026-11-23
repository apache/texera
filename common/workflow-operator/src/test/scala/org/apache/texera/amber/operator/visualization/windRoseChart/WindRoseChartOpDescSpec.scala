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

package org.apache.texera.amber.operator.visualization.windRoseChart

import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.texera.amber.operator.GeneratedCodeAssertionChecks

import scala.jdk.CollectionConverters._

class WindRoseChartOpDescSpec
    extends AnyFlatSpec
    with BeforeAndAfter
    with GeneratedCodeAssertionChecks {

  var opDesc: WindRoseChartOpDesc = _

  before {
    opDesc = new WindRoseChartOpDesc()
  }

  it should "default rColumn and thetaColumn to non-null, empty strings" in {
    opDesc.rColumn should not be null
    opDesc.rColumn shouldBe ""
    opDesc.thetaColumn should not be null
    opDesc.thetaColumn shouldBe ""
  }

  it should "throw an AssertionError (not a NullPointerException) naming the Radial column when rColumn is empty" in {
    expectError[AssertionError]("Radial Values (r) column must be selected.")(
      opDesc.createPlotlyFigure()
    )
  }

  it should "throw an AssertionError naming the Angular column when only rColumn is set" in {
    opDesc.rColumn = "r_col"
    expectError[AssertionError]("Angular Values (θ) column must be selected.")(
      opDesc.createPlotlyFigure()
    )
  }

  it should "render the configured r and theta columns when both are set" in {
    opDesc.rColumn = "r_col"
    opDesc.thetaColumn = "theta_col"

    val figurePlain = opDesc.createPlotlyFigure().plain
    assert(carries(figurePlain, "r_col"))
    assert(carries(figurePlain, "theta_col"))
    figurePlain should include("px.bar_polar")

    val code = opDesc.generatePythonCode()
    assert(carries(code, "r_col"))
    assert(carries(code, "theta_col"))
    code should include("class ProcessTableOperator(UDFTableOperator)")
  }

  "WindRoseChartOpDesc's generated schema" should
    "constrain the radial values to numeric and leave the angle unconstrained" in {
    // The angle is a direction label — "N", "NE" — so a rule there would reject
    // the ordinary case. Only the wedge length has to be a number.
    val schema = OperatorMetadataGenerator.generateOperatorJsonSchema(classOf[WindRoseChartOpDesc])
    val rules = schema.path("attributeTypeRules")
    rules.fieldNames().asScala.toSet shouldBe Set("rColumn")

    // The key has to name a property the form actually renders; one that names
    // nothing parses fine and constrains nothing.
    schema.path("properties").has("rColumn") shouldBe true

    val allowed = rules.path("rColumn").path("enum").elements().asScala.map(_.asText()).toSet
    allowed shouldBe Set("integer", "long", "double")
    allowed should not contain "string"
  }
}
