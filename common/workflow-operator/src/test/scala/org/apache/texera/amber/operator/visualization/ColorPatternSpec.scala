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
  * Covers the colour `pattern` the visualization settings inject into their schema, one
  * test per field per value.
  *
  * The values are the shapes plotly's ColorValidator accepts, one per branch of the
  * pattern, plus the near-misses it rejects. A colour name is matched lexically, so
  * `red` stands for the branch rather than for the CSS list.
  */
class ColorPatternSpec extends AnyFlatSpec with Matchers {

  /** A value the field should take, and whether the pattern is meant to admit it. */
  private val colorValues: Seq[(String, Boolean)] = Seq(
    "" -> true, // blank means the operator omits the colour argument
    "#fff" -> true,
    "#FFFFFF" -> true,
    "#ff ffff" -> true, // plotly strips spaces before matching
    "rgb(255, 0, 0)" -> true,
    "rgba(255, 0, 0, 0.5)" -> true,
    "hsl(120, 50%, 50%)" -> true,
    "hsva(120, 50%, 50%, 0.5)" -> true,
    "var(--my-color)" -> true,
    "red" -> true,
    "1" -> false,
    "#12" -> false,
    "#ggg" -> false,
    "#ffff" -> false,
    "rgb(1,2)" -> false,
    "rgb(-1,2,3)" -> false
  )

  // Every field carrying the colour pattern, as (label, operator, path to the property).
  // BandConfig appears twice: it declares fillColor and inherits color from LineConfig.
  private val colorFields: Seq[(String, Class[_ <: LogicalOp], Seq[String])] = Seq(
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

  private def patternOf(opDescClass: Class[_ <: LogicalOp], path: Seq[String]): Option[String] = {
    val property = path.foldLeft(OperatorMetadataGenerator.generateOperatorJsonSchema(opDescClass))(
      (node, segment) => node.path(segment)
    )
    Option.when(property.has("pattern"))(property.path("pattern").asText())
  }

  // Read every schema once, up front, rather than once per case below.
  private val colorPatterns: Seq[(String, Option[String])] = colorFields.map {
    case (label, opDescClass, path) => label -> patternOf(opDescClass, path)
  }

  private def describe(value: String): String =
    if (value.isEmpty) "a blank value" else s"'$value'"

  colorPatterns.foreach {
    case (label, pattern) =>
      behavior of s"The colour pattern on $label"

      it should "be present in the generated schema" in {
        pattern shouldBe defined
      }

      colorValues.foreach {
        case (value, isValid) =>
          val verb = if (isValid) "accept" else "reject"
          it should s"$verb ${describe(value)}" in {
            val regex = Pattern.compile(pattern.getOrElse(fail(s"$label carries no pattern")))
            // find() rather than matches(), because the form validates with
            // `new RegExp().test`, which searches instead of anchoring.
            regex.matcher(value).find() shouldBe isValid
          }
      }
  }

  behavior of "The colour pattern"

  it should "read identically from every schema, so the copies cannot drift" in {
    colorPatterns.map { case (_, pattern) => pattern }.distinct should have size 1
  }
}
