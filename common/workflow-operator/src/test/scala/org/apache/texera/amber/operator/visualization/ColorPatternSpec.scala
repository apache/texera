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

package org.apache.texera.amber.operator.visualization

import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.apache.texera.amber.operator.visualization.continuousErrorBands.ContinuousErrorBandsOpDesc
import org.apache.texera.amber.operator.visualization.figureFactoryTable.FigureFactoryTableOpDesc
import org.apache.texera.amber.operator.visualization.lineChart.LineChartOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.regex.Pattern

/**
  * Covers the colour `pattern` the visualization settings inject into their schema.
  *
  * The values are the shapes plotly's ColorValidator accepts, one per branch of the
  * pattern, plus the near-misses it rejects. A colour name is matched lexically, so
  * `red` stands for the branch rather than for the CSS list.
  */
class ColorPatternSpec extends AnyFlatSpec with Matchers {

  private val accepted = Seq(
    "",
    "#fff",
    "#FFFFFF",
    "#ff ffff", // plotly strips spaces before matching
    "rgb(255, 0, 0)",
    "rgba(255, 0, 0, 0.5)",
    "hsl(120, 50%, 50%)",
    "hsva(120, 50%, 50%, 0.5)",
    "var(--my-color)",
    "red"
  )

  private val rejected = Seq("1", "#12", "#ggg", "#ffff", "rgb(1,2)", "rgb(-1,2,3)")

  // Every field carrying the colour pattern, as (label, operator, path to the property).
  // BandConfig appears twice: it declares fillColor and inherits color from LineConfig.
  private val colorFields = Seq(
    (
      "LineConfig.color",
      classOf[LineChartOpDesc],
      Seq("definitions", "LineConfig", "properties", "color")
    ),
    (
      "BandConfig.fillColor",
      classOf[ContinuousErrorBandsOpDesc],
      Seq("definitions", "BandConfig", "properties", "fillColor")
    ),
    (
      "BandConfig.color",
      classOf[ContinuousErrorBandsOpDesc],
      Seq("definitions", "BandConfig", "properties", "color")
    ),
    (
      "FigureFactoryTableOpDesc.fontColor",
      classOf[FigureFactoryTableOpDesc],
      Seq("properties", "fontColor")
    )
  )

  private def patternOf(opDescClass: Class[_ <: LogicalOp], path: Seq[String]): String = {
    val property = path.foldLeft(OperatorMetadataGenerator.generateOperatorJsonSchema(opDescClass))(
      (node, segment) => node.path(segment)
    )
    withClue(s"${opDescClass.getSimpleName} ${path.mkString(".")} carries no pattern: ") {
      property.has("pattern") shouldBe true
    }
    property.path("pattern").asText()
  }

  "The colour settings" should "accept and reject the same values in every schema" in {
    colorFields.foreach {
      case (label, opDescClass, path) =>
        // find() rather than matches(), because the form validates with `new RegExp().test`.
        val pattern = Pattern.compile(patternOf(opDescClass, path))
        accepted.foreach { value =>
          withClue(s"$label should accept '$value': ") {
            pattern.matcher(value).find() shouldBe true
          }
        }
        rejected.foreach { value =>
          withClue(s"$label should reject '$value': ") {
            pattern.matcher(value).find() shouldBe false
          }
        }
    }
  }

  it should "state the pattern identically, so the three copies cannot drift" in {
    val patterns = colorFields.map { case (_, opDescClass, path) => patternOf(opDescClass, path) }
    patterns.distinct should have size 1
  }
}
