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

package org.apache.texera.amber.operator.recode

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.executor.{ExecFactory, OpExecWithClassName, OperatorExecutor}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, SchemaEnforceable, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.IteratorHasAsScala

/** Exercise the same JSON, schema propagation and executor boundaries as saved workflows. */
class CategoryRecodeOpSpec extends AnyFlatSpec with Matchers {
  private def schema(kind: AttributeType = AttributeType.STRING): Schema =
    Schema().add("code", kind).add("row", AttributeType.INTEGER)

  private def descriptor(
      mappings: List[(String, String)] = List("F" -> "Female", "M" -> "Male"),
      options: Map[String, Any] = Map.empty
  ): LogicalOp = {
    val config: Map[String, Any] = Map(
      "operatorType" -> "CategoryRecode",
      "attribute" -> "code",
      "outputAttribute" -> "category",
      "mappings" -> mappings.map {
        case (value, category) =>
          Map("value" -> value, "category" -> category)
      }
    ) ++ options
    val json = objectMapper.valueToTree[ObjectNode](config)
    // The application's NON_NULL writer omits null map entries. These test cases must send
    // explicit JSON null, not accidentally test an omitted field with its default value.
    options.foreach { case (key, value) => if (value == null) json.putNull(key) }
    objectMapper.treeToValue(json, classOf[LogicalOp])
  }

  private def outputSchema(desc: LogicalOp, input: Schema = schema()): Schema =
    desc.getExternalOutputSchemas(Map(PortIdentity() -> input)).values.head

  private def executorFor(desc: LogicalOp): OperatorExecutor = {
    desc.getPhysicalOp(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID).opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.recode.CategoryRecodeOpExec"
        ExecFactory.newExecFromJavaClassName(className, descString)
      case other => fail(s"Expected a native executor, got $other")
    }
  }

  private def run(desc: LogicalOp, values: Seq[Any], input: Schema = schema()): Seq[Tuple] = {
    val output = outputSchema(desc, input)
    val executor = executorFor(desc)
    executor.open()
    try {
      values.zipWithIndex.flatMap {
        case (value, index) =>
          val tuple = Tuple(input, Array[Any](value, index))
          val before = tuple.getFields.clone()
          val result = executor
            .processTuple(tuple, 0)
            .map {
              _.asInstanceOf[SchemaEnforceable].enforceSchema(output)
            }
            .toList
          tuple.getFields.toSeq shouldBe before.toSeq
          result
      }
    } finally executor.close()
  }

  private def categories(desc: LogicalOp, values: Seq[Any], input: Schema = schema()): Seq[String] =
    run(desc, values, input).map(_.getField[String]("category"))

  "Category Recode" should "append a string category without changing rows or source columns" in {
    val desc = descriptor()
    val result = run(desc, Seq("F", "M", "F"))
    outputSchema(desc).getAttributes.dropRight(1) shouldBe schema().getAttributes
    outputSchema(desc).getAttribute("category").getType shouldBe AttributeType.STRING
    result.map(_.getField[String]("category")) shouldBe Seq("Female", "Male", "Female")
    result.map(_.getField[String]("code")) shouldBe Seq("F", "M", "F")
    result.map(_.getField[Int]("row")) shouldBe Seq(0, 1, 2)
  }

  it should "keep string case, whitespace, empty strings and unicode distinct" in {
    val desc = descriptor(
      List(
        "F" -> "upper",
        "f" -> "lower",
        " F " -> "padded",
        "" -> "blank",
        "北" -> "中文",
        "é" -> "café",
        "null" -> "literal"
      )
    )
    categories(desc, Seq("F", "f", " F ", "", "北", "é", "null", null, "F ")) shouldBe
      Seq("upper", "lower", "padded", "blank", "中文", "café", "literal", null, null)
  }

  it should "allow many values in one category and preserve an explicitly empty category" in {
    categories(
      descriptor(List("A" -> "shared", "B" -> "shared", "C" -> "")),
      Seq("B", "A", "C")
    ) shouldBe Seq("shared", "shared", "")
  }

  it should "emit no rows for empty input and use null for unmatched values by default" in {
    run(descriptor(), Seq.empty) shouldBe empty
    categories(descriptor(), Seq("unknown", null)) shouldBe Seq(null, null)
  }

  it should "handle each unmatched policy without conflating null and unmatched data" in {
    val values = Seq("F", "unknown", null)
    categories(descriptor(options = Map("unmatched" -> "Keep original")), values) shouldBe
      Seq("Female", "unknown", null)
    categories(
      descriptor(options = Map("unmatched" -> "Use default", "defaultCategory" -> "Other")),
      values
    ) shouldBe Seq("Female", "Other", null)
    categories(
      descriptor(options = Map("unmatched" -> "Use default", "defaultCategory" -> "")),
      Seq("unknown")
    ) shouldBe Seq("")
    val strict = descriptor(options = Map("unmatched" -> "Error"))
    categories(strict, Seq("F", null)) shouldBe Seq("Female", null)
    intercept[IllegalArgumentException](
      categories(strict, Seq("private-unmatched-value"))
    ).getMessage should include("Unmatched value")
    intercept[IllegalArgumentException](
      categories(strict, Seq("private-unmatched-value"))
    ).getMessage should not include "private-unmatched-value"
  }

  it should "use an explicit missing category independently of all unmatched policies" in {
    Seq("Set null", "Keep original", "Use default", "Error").foreach { policy =>
      val desc = descriptor(options =
        Map("unmatched" -> policy, "defaultCategory" -> "Other", "missingCategory" -> "Missing")
      )
      categories(desc, Seq(null, "F")) shouldBe Seq("Missing", "Female")
    }
  }

  Seq(
    (AttributeType.INTEGER, "2147483647", Int.MaxValue),
    (AttributeType.INTEGER, "-2147483648", Int.MinValue),
    (AttributeType.LONG, "9223372036854775807", Long.MaxValue),
    (AttributeType.LONG, "-9223372036854775808", Long.MinValue),
    (AttributeType.DOUBLE, "1.25e2", 125.0),
    (AttributeType.DOUBLE, "-0", 0.0),
    (AttributeType.BOOLEAN, "true", true),
    (AttributeType.BOOLEAN, "false", false)
  ).foreach {
    case (kind, text, value) =>
      it should s"match a typed $kind key $text" in {
        categories(descriptor(List(text -> "matched")), Seq(value, null), schema(kind)) shouldBe
          Seq("matched", null)
      }
  }

  it should "render unmatched numbers and booleans as text while preserving their input types" in {
    categories(
      descriptor(List("1" -> "one"), Map("unmatched" -> "Keep original")),
      Seq(2L),
      schema(AttributeType.LONG)
    ) shouldBe Seq("2")
    categories(
      descriptor(List("true" -> "yes"), Map("unmatched" -> "Keep original")),
      Seq(false),
      schema(AttributeType.BOOLEAN)
    ) shouldBe Seq("false")
    categories(
      descriptor(List("0" -> "zero"), Map("unmatched" -> "Keep original")),
      Seq(1.25),
      schema(AttributeType.DOUBLE)
    ) shouldBe Seq("1.25")
  }

  it should "validate source and output names before processing data" in {
    Seq(null, "", "absent").foreach { name =>
      intercept[IllegalArgumentException](
        outputSchema(descriptor(options = Map("attribute" -> name)))
      ).getMessage should include("Source column")
    }
    Seq(null, "", "  ", "code", "CODE", "row").foreach { name =>
      intercept[IllegalArgumentException](
        outputSchema(descriptor(options = Map("outputAttribute" -> name)))
      ).getMessage should include("Output column")
    }
    outputSchema(descriptor(options = Map("attribute" -> "CODE"))).getAttributeNames shouldBe List(
      "code",
      "row",
      "category"
    )
  }

  it should "reject empty, missing and malformed mapping configurations" in {
    Seq[Any](
      null,
      List.empty,
      List(null),
      List(Map("category" -> "label")),
      List(Map("value" -> "F")),
      List(Map("value" -> null, "category" -> "label")),
      List(Map("value" -> "F", "category" -> null))
    ).foreach { rules =>
      intercept[IllegalArgumentException](
        outputSchema(descriptor(options = Map("mappings" -> rules)))
      ).getMessage.toLowerCase should include("mapping")
    }
    intercept[IllegalArgumentException](
      outputSchema(descriptor(options = Map("unmatched" -> null)))
    ).getMessage should include("Unmatched")
    intercept[IllegalArgumentException](
      outputSchema(descriptor(options = Map("unmatched" -> "Use default")))
    ).getMessage should include("Default category")
    intercept[com.fasterxml.jackson.databind.JsonMappingException] {
      descriptor(options = Map("unmatched" -> "unexpected-policy"))
    }
  }

  Seq(
    (AttributeType.STRING, "A", "A"),
    (AttributeType.INTEGER, "1", "01"),
    (AttributeType.LONG, "0", "+0"),
    (AttributeType.DOUBLE, "1", "1e0"),
    (AttributeType.DOUBLE, "-0.0", "0.0"),
    (AttributeType.BOOLEAN, "true", "true")
  ).foreach {
    case (kind, first, second) =>
      it should s"reject duplicate typed $kind mappings $first and $second" in {
        intercept[IllegalArgumentException] {
          outputSchema(descriptor(List(first -> "a", second -> "b")), schema(kind))
        }.getMessage should include("Duplicate mapping")
      }
  }

  Seq(
    AttributeType.INTEGER -> Seq("2147483648", "-2147483649", "1.2", "1x", " 1", ""),
    AttributeType.LONG -> Seq("9223372036854775808", "-9223372036854775809", "1.0", ""),
    AttributeType.DOUBLE -> Seq("NaN", "Infinity", "1e309", "1e-999", "1.0f", "0x1.0p0", "1 ", ""),
    AttributeType.BOOLEAN -> Seq("TRUE", "False", "yes", "1", "", " true")
  ).foreach {
    case (kind, texts) =>
      it should s"reject malformed or out-of-range $kind keys instead of coercing them" in {
        texts.foreach { text =>
          intercept[IllegalArgumentException] {
            outputSchema(descriptor(List(text -> "bad")), schema(kind))
          }.getMessage should include("Mapping value")
        }
      }
  }

  it should "reject unsupported source types and nonfinite runtime values" in {
    Seq(
      AttributeType.TIMESTAMP,
      AttributeType.BINARY,
      AttributeType.LARGE_BINARY,
      AttributeType.ANY
    )
      .foreach { kind =>
        intercept[IllegalArgumentException](
          outputSchema(descriptor(), schema(kind))
        ).getMessage should include("Source column type")
      }
    Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).foreach { value =>
      intercept[IllegalArgumentException] {
        categories(descriptor(List("1" -> "one")), Seq(value), schema(AttributeType.DOUBLE))
      }.getMessage should include("Nonfinite")
    }
  }

  it should "round-trip saved JSON and expose a discoverable native form" in {
    val desc = descriptor(options =
      Map(
        "unmatched" -> "Use default",
        "defaultCategory" -> "Other",
        "missingCategory" -> "Missing"
      )
    )
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(desc), classOf[LogicalOp])
    categories(restored, Seq("F", "unknown", null)) shouldBe Seq("Female", "Other", "Missing")
    restored shouldBe desc
    val metadata = OperatorMetadataGenerator.allOperatorMetadata.operators
      .find(_.operatorType == "CategoryRecode")
      .get
    metadata.additionalMetadata.inputPorts.size shouldBe 1
    metadata.additionalMetadata.outputPorts.size shouldBe 1
    val properties = metadata.jsonSchema.path("properties")
    properties.path("attribute").path("autofill").asText shouldBe "attributeName"
    properties.path("mappings").path("type").asText shouldBe "array"
    properties.path("mappings").path("minItems").asInt shouldBe 1
    properties.path("unmatched").path("enum").elements().asScala.map(_.asText()).toSet shouldBe
      Set("Set null", "Keep original", "Use default", "Error")
  }

  it should "reject numeric ordinals and malformed JSON policy values" in {
    Seq[Any](0, 1, true, List("Set null"), Map("policy" -> "Set null"), "set null").foreach {
      policy =>
        withClue(s"Invalid policy $policy: ") {
          intercept[com.fasterxml.jackson.databind.JsonMappingException] {
            descriptor(options = Map("unmatched" -> policy))
          }
        }
    }
  }

  it should "revalidate a worker's schema and never reuse an incompatible compiled mapping" in {
    val desc = descriptor(List("1" -> "one"))
    val executor = executorFor(desc)
    executor.open()
    try {
      Seq(schema(), schema(AttributeType.INTEGER), schema(AttributeType.LONG)).foreach { input =>
        val value: Any = input.getAttribute("code").getType match {
          case AttributeType.STRING  => "1"
          case AttributeType.INTEGER => 1
          case _                     => 1L
        }
        val output = executor
          .processTuple(Tuple(input, Array[Any](value, 0)), 0)
          .next()
          .asInstanceOf[SchemaEnforceable]
          .enforceSchema(outputSchema(desc, input))
        output.getField[String]("category") shouldBe "one"
      }
      intercept[IllegalArgumentException] {
        executor.processTuple(Tuple(schema(AttributeType.BOOLEAN), Array[Any](true, 0)), 0).next()
      }.getMessage should include("Mapping value")
    } finally executor.close()
  }

  it should "validate malformed configurations in the executor even if schema propagation was skipped" in {
    val executor = executorFor(descriptor(List("F" -> "first", "F" -> "second")))
    executor.open()
    try {
      intercept[IllegalArgumentException] {
        executor.processTuple(Tuple(schema(), Array[Any]("F", 0)), 0).next()
      }.getMessage should include("Duplicate mapping")
    } finally executor.close()
  }
}
