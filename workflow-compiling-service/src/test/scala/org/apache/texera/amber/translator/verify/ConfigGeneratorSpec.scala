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

package org.apache.texera.amber.translator.verify

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.operator.filter.SpecializedFilterOpDesc
import org.apache.texera.amber.operator.hashJoin.HashJoinOpDesc
import org.apache.texera.amber.operator.intersect.IntersectOpDesc
import org.apache.texera.amber.operator.regex.RegexOpDesc
import org.apache.texera.amber.operator.unneststring.UnnestStringOpDesc
import org.apache.texera.amber.operator.visualization.candlestickChart.CandlestickChartOpDesc
import org.apache.texera.amber.operator.visualization.choroplethMap.ChoroplethMapOpDesc
import org.apache.texera.amber.operator.visualization.histogram2d.Histogram2DOpDesc
import org.apache.texera.amber.operator.visualization.radarChart.RadarChartOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Drives [[ConfigGenerator]] across the range of operator config shapes:
  * no-config, flat autofill + enum, and a nested list of objects with a
  * free-form value. These are the cases the reflective generator must handle to
  * cover the JVM-exec operators automatically.
  */
class ConfigGeneratorSpec extends AnyFlatSpec with Matchers {

  private val schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING),
    new Attribute("score", AttributeType.DOUBLE)
  )
  private val twoPorts = Map(0 -> schema, 1 -> schema)

  "ConfigGenerator" should "configure an operator that has no config fields" in {
    val result = ConfigGenerator.generate(classOf[IntersectOpDesc], twoPorts)
    withClue(result) { result.isRight shouldBe true }
    result.toOption.get shouldBe a[IntersectOpDesc]
  }

  it should "fill autofill column refs from the correct port and default the enum" in {
    val result = ConfigGenerator.generate(classOf[HashJoinOpDesc[Any]], twoPorts)
    withClue(result) { result.isRight shouldBe true }
    val op = result.toOption.get.asInstanceOf[HashJoinOpDesc[Any]]
    schema.getAttributeNames should contain(op.buildAttributeName)
    schema.getAttributeNames should contain(op.probeAttributeName)
    op.joinType should not be null
  }

  it should "build a non-empty nested predicate list with a valid column and enum" in {
    val result = ConfigGenerator.generate(classOf[SpecializedFilterOpDesc], Map(0 -> schema))
    result.isRight shouldBe true
    val op = result.toOption.get.asInstanceOf[SpecializedFilterOpDesc]
    op.predicates should not be empty
    val p = op.predicates.head
    schema.getAttributeNames should contain(p.attribute)
    p.condition should not be null
  }

  // ── semantic column resolution (the @SampleColumn / attributeTypeRules tiers) ──

  it should "fill @SampleColumn-tagged fields with the named semantic columns" in {
    val result =
      ConfigGenerator.generate(classOf[CandlestickChartOpDesc], CanonicalFixture.schemasByPort)
    withClue(result) { result.isRight shouldBe true }
    val op = result.toOption.get.asInstanceOf[CandlestickChartOpDesc]
    op.open.toString shouldBe "open"
    op.high.toString shouldBe "high"
    op.low.toString shouldBe "low"
    op.close.toString shouldBe "close"
    op.date.toString shouldBe "trade_date"
  }

  it should "assign distinct columns to sibling autofill fields (no x = y collapse)" in {
    // Histogram2D's xColumn and yColumn are both plain @AutofillAttributeName
    // with no type-rule; before distinct-aware binding both resolved to the
    // first column ("id"). They must now differ so the plot isn't a degenerate
    // diagonal.
    val result =
      ConfigGenerator.generate(classOf[Histogram2DOpDesc], CanonicalFixture.schemasByPort)
    withClue(result) { result.isRight shouldBe true }
    val op = result.toOption.get.asInstanceOf[Histogram2DOpDesc]
    op.xColumn.toString should not be empty
    op.xColumn.toString should not be op.yColumn.toString
  }

  it should "keep a list knob off the column a single-column sibling took" in {
    // Radar Chart picks its name column first and its value columns took the whole
    // table, so the name arrived inside them and the generated `required_cols`,
    // which is the name followed by the values, named it twice. More than one value
    // column left, or the list runs the same code a single column would.
    val result =
      ConfigGenerator.generate(classOf[RadarChartOpDesc], CanonicalFixture.schemasByPort)
    withClue(result) { result.isRight shouldBe true }
    val op = result.toOption.get.asInstanceOf[RadarChartOpDesc]
    op.valueColumns.map(_.toString) should not contain op.nameColumn.toString
    op.valueColumns.size should be > 1
  }

  it should "pick a type-matching column from attributeTypeRules, and a @SampleColumn for ISO codes" in {
    val result =
      ConfigGenerator.generate(classOf[ChoroplethMapOpDesc], CanonicalFixture.schemasByPort)
    withClue(result) { result.isRight shouldBe true }
    val op = result.toOption.get.asInstanceOf[ChoroplethMapOpDesc]
    op.locations.toString shouldBe "iso_country" // via @SampleColumn
    // color is constrained to integer|long|double by attributeTypeRules, so it
    // must resolve to a numeric column rather than the first (string) column.
    Set("id", "score", "open", "high", "low", "close") should contain(op.color.toString)
  }

  private def variants(
      opClass: Class[_ <: org.apache.texera.amber.operator.LogicalOp],
      ports: Map[Int, Schema]
  ): Seq[(String, org.apache.texera.amber.operator.LogicalOp)] = {
    val result = ConfigGenerator.generateVariants(opClass, ports, 15)
    withClue(result) { result.isRight shouldBe true }
    result.toOption.get
  }

  "generateVariants" should "offer every value an enum can take, one per variant" in {
    val vs = variants(classOf[HashJoinOpDesc[Any]], twoPorts)
    val chosen = vs.map { case (_, op) => op.asInstanceOf[HashJoinOpDesc[Any]].joinType.toString }
    // One variant per value, and no value twice: the sweep is linear over the
    // enum rather than a product with anything else.
    chosen.distinct.size shouldBe chosen.size
    chosen.size should be > 1
    vs.map(_._1).head shouldBe "default"
    // Every non-default variant names the field it moved, so a failure points at
    // the knob rather than at the operator. The value in the label is the one the
    // form offers, which is not the enum constant's own name.
    vs.tail.map(_._1).foreach(_ should startWith("joinType="))
    vs.map(_._1).distinct.size shouldBe vs.size
  }

  it should "flip a boolean knob in a variant of its own" in {
    val vs = variants(classOf[RegexOpDesc], Map(0 -> CanonicalFixture.schema))
    val (_, base) = vs.head
    base.asInstanceOf[RegexOpDesc].caseInsensitive shouldBe false
    val flipped = vs.collectFirst { case ("caseInsensitive=true", op) => op }
    withClue(vs.map(_._1)) { flipped shouldBe defined }
    flipped.get.asInstanceOf[RegexOpDesc].caseInsensitive shouldBe true
  }

  it should "fill in one variant the optional knobs the base configuration leaves empty" in {
    val vs = variants(classOf[SpecializedFilterOpDesc], Map(0 -> CanonicalFixture.schema))
    // `value` is optional, so the base configuration does not set it.
    vs.head._2.asInstanceOf[SpecializedFilterOpDesc].predicates.head.value shouldBe null
    val optionals = vs.collectFirst {
      case (label, op) if label.startsWith("optionals") => op.asInstanceOf[SpecializedFilterOpDesc]
    }
    withClue(vs.map(_._1)) { optionals shouldBe defined }
    optionals.get.predicates.head.value should not be null
  }

  it should "give each free-text knob its own hostile value, so two cannot collide" in {
    val vs = variants(classOf[UnnestStringOpDesc], Map(0 -> CanonicalFixture.schema))
    val hostile = vs.collectFirst {
      case (label, op) if label.startsWith("hostileText") => op.asInstanceOf[UnnestStringOpDesc]
    }
    withClue(vs.map(_._1)) { hostile shouldBe defined }
    // The first knob carries the bare value and the next is numbered. Sharing one
    // value across both would have them write the same column name.
    hostile.get.delimiter shouldBe "a\"b"
    hostile.get.resultAttribute shouldBe "a\"b2"
  }
}
