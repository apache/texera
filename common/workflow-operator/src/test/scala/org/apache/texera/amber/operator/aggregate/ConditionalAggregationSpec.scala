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

package org.apache.texera.amber.operator.aggregate

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.executor.{ExecFactory, OpExecWithClassName}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, SchemaEnforceable, Tuple}
import org.apache.texera.amber.core.workflow.{PhysicalOp, PortIdentity}
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.operator.metadata.OperatorMetadataGenerator
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import scala.jdk.CollectionConverters.IteratorHasAsScala

class ConditionalAggregationSpec extends AnyFlatSpec with Matchers {
  private val inputSchema = Schema()
    .add("year", AttributeType.INTEGER)
    .add("sex", AttributeType.STRING)
    .add("race", AttributeType.STRING)
    .add("age", AttributeType.DOUBLE)
    .add("amount", AttributeType.INTEGER)

  private val rows = Seq(
    Tuple(inputSchema, Array[Any](2013, "F", "BLACK/AFRICAN AMERICAN", 18.0, 10)),
    Tuple(inputSchema, Array[Any](2013, "M", "WHITE", 49.0, 20)),
    Tuple(inputSchema, Array[Any](2013, "F", "OTHER", 17.0, null)),
    Tuple(inputSchema, Array[Any](2014, "M", "BLACK", 65.0, 30)),
    Tuple(inputSchema, Array[Any](2015, "F", "WHITE", 80.0, null)),
    Tuple(inputSchema, Array[Any](null, null, null, null, 0))
  )

  private def rule(attribute: String, condition: String, value: String = null): Map[String, Any] =
    Map("attribute" -> attribute, "condition" -> condition, "value" -> value)

  private def measure(
      name: String,
      conditions: Seq[Map[String, Any]] = Seq.empty,
      function: String = "count",
      column: String = "",
      mode: String = "all"
  ): Map[String, Any] =
    Map(
      "aggFunction" -> function,
      "attribute" -> column,
      "result attribute" -> name,
      "conditions" -> conditions,
      "conditionMatch" -> mode
    )

  private def descriptor(
      measures: Seq[Map[String, Any]],
      keys: Seq[String] = Seq("year")
  ): AggregateOpDesc = {
    val json = objectMapper.valueToTree[ObjectNode](
      Map("operatorType" -> "Aggregate", "groupByKeys" -> keys, "aggregations" -> measures)
    )
    measures.zipWithIndex.foreach {
      case (operation, index) =>
        operation.foreach {
          case (key, value) =>
            if (value == null)
              json.path("aggregations").get(index).asInstanceOf[ObjectNode].putNull(key)
        }
    }
    objectMapper.treeToValue(json, classOf[AggregateOpDesc])
  }

  private def runStage(op: PhysicalOp, data: Seq[Tuple]): Seq[Tuple] = {
    val outputSchema = op.outputPorts.values.head._3.fold(throw _, identity)
    val executor = op.opExecInitInfo match {
      case OpExecWithClassName(name, config) => ExecFactory.newExecFromJavaClassName(name, config)
      case other                             => fail(s"Expected native aggregate executor: $other")
    }
    executor.open()
    try {
      data.foreach(row => executor.processTuple(row, 0).toList shouldBe empty)
      executor.onFinish(0).map(_.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema)).toList
    } finally executor.close()
  }

  private def run(
      desc: AggregateOpDesc,
      partitions: Seq[Seq[Tuple]],
      schema: Schema = inputSchema
  ): Seq[Tuple] = {
    val plan = desc
      .getPhysicalPlan(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)
      .propagateSchema(Map(PortIdentity() -> schema))
    val local = plan.operators.find(_.inputPorts.keys.exists(!_.internal)).get
    val global = plan.operators.find(_.inputPorts.keys.forall(_.internal)).get
    runStage(global, partitions.flatMap(runStage(local, _)))
  }

  private def validate(operation: Map[String, Any], schema: Schema = inputSchema): Unit = {
    val plan = descriptor(Seq(operation), Seq.empty)
      .getPhysicalPlan(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)
      .propagateSchema(Map(PortIdentity() -> schema))
    // The global stage has no schema when the local stage fails. Inspect the
    // original validation error, not its downstream "schema unavailable" result.
    plan.operators
      .find(_.inputPorts.keys.exists(!_.internal))
      .get
      .outputPorts
      .values
      .head
      ._3
      .fold(throw _, _ => ())
  }

  "Conditional aggregation" should "calculate independent measures in one aggregate and preserve zero groups" in {
    val desc = descriptor(
      Seq(
        measure("n"),
        measure("female", Seq(rule("sex", "=", "F"))),
        measure("female_black", Seq(rule("sex", "=", "F"), rule("race", "contains", "BLACK"))),
        measure("adult_total", Seq(rule("age", ">=", "18")), "sum", "amount"),
        measure("nonnull_female", Seq(rule("sex", "=", "F")), column = "amount")
      )
    )
    val result = run(desc, Seq(rows.take(2), rows.drop(2), Seq.empty)).map { row =>
      row.getField[Any]("year") -> row.getFields.drop(1).toSeq
    }.toMap
    result shouldBe Map(
      2013 -> Seq(3, 2, 1, 30, 1),
      2014 -> Seq(1, 0, 0, 30, 0),
      2015 -> Seq(1, 1, 0, 0, 0),
      (null: Any) -> Seq(1, 0, 0, 0, 0)
    )
  }

  it should "support all and any matching without double-counting overlapping rules" in {
    val predicates = Seq(rule("sex", "=", "F"), rule("race", "contains", "BLACK"))
    val desc = descriptor(
      Seq(measure("both", predicates), measure("either", predicates, mode = "any")),
      Seq.empty
    )
    run(desc, Seq(rows)).head.getFields.toSeq shouldBe Seq(1, 4)
  }

  it should "merge partition totals without reapplying source predicates" in {
    val operation = measure("adults", Seq(rule("age", ">", "17")))
    val one = run(descriptor(Seq(operation)), Seq(rows))
    val many = run(descriptor(Seq(operation)), Seq(Seq.empty) ++ rows.reverse.map(row => Seq(row)))
    one.toSet shouldBe many.toSet
    one.find(_.getField[Any]("year") == 2013).get.getField[Int]("adults") shouldBe 2
  }

  it should "leave saved descriptors intact across repeated schema checks and planning" in {
    val desc = descriptor(Seq(measure("female", Seq(rule("sex", "=", "F")))))
    val before = objectMapper.readTree(objectMapper.writeValueAsString(desc))
    (1 to 3).foreach { _ =>
      desc.getExternalOutputSchemas(Map(PortIdentity() -> inputSchema))
      desc.getPhysicalPlan(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID)
      objectMapper.readTree(objectMapper.writeValueAsString(desc)) shouldBe before
    }
    run(desc, Seq(rows))
      .find(_.getField[Any]("year") == 2013)
      .get
      .getField[Int]("female") shouldBe 2
  }

  it should "preserve legacy absent-condition and empty-input behavior" in {
    val operation = Map[String, Any]("aggFunction" -> "count", "result attribute" -> "n")
    run(descriptor(Seq(operation), Seq.empty), Seq(rows)).head.getField[Int]("n") shouldBe 6
    run(descriptor(Seq(operation), Seq.empty), Seq(Seq.empty)) shouldBe empty
    run(descriptor(Seq(measure("n", mode = "any")), Seq.empty), Seq(rows)).head
      .getField[Int]("n") shouldBe 6
  }

  it should "exclude null predicate values except explicit null checks and retain SUM null identity" in {
    val desc = descriptor(
      Seq(
        measure("null_sex", Seq(rule("sex", "is null"))),
        measure("nonnull_sex", Seq(rule("sex", "is not null"))),
        measure("not_female", Seq(rule("sex", "!=", "F"))),
        measure("not_black", Seq(rule("race", "not contains", "BLACK"))),
        measure("sum_nulls", Seq(rule("amount", "is null")), "sum", "amount")
      ),
      Seq.empty
    )
    run(desc, Seq(rows)).head.getFields.toSeq shouldBe Seq(1, 5, 2, 3, 0)
  }

  it should "handle age boundaries without excluding fractional ages incorrectly" in {
    val ages = Seq(0.0, 17.0, 17.5, 18.0, 24.0, 24.5, 44.0, 44.5, 49.0, 49.5, 64.0, 64.5, null)
    val data = ages.map(age => Tuple(inputSchema, Array[Any](2013, "F", "WHITE", age, 1)))
    val desc = descriptor(
      Seq(
        measure("a", Seq(rule("age", ">", "17"), rule("age", "<=", "24"))),
        measure("b", Seq(rule("age", ">", "24"), rule("age", "<=", "44"))),
        measure("c", Seq(rule("age", ">", "44"), rule("age", "<=", "49"))),
        measure("d", Seq(rule("age", ">", "49"), rule("age", "<=", "64"))),
        measure("e", Seq(rule("age", ">", "64")))
      ),
      Seq.empty
    )
    run(desc, Seq(data)).head.getFields.toSeq shouldBe Seq(3, 2, 2, 2, 1)
  }

  private def countValues(
      kind: AttributeType,
      values: Seq[Any],
      condition: String,
      value: String
  ): Int = {
    val schema = Schema().add("v", kind)
    val data = values.map(v => Tuple(schema, Array[Any](v)))
    val desc = descriptor(Seq(measure("n", Seq(rule("v", condition, value)))), Seq.empty)
    run(desc, Seq(data), schema).head.getField[Int]("n")
  }

  it should "compare text exactly including unicode, whitespace and numeric-looking strings" in {
    val data = Seq("F", "f", " F ", "01", "1", "北", "")
    Seq("F", "f", " F ", "1", "北", "").foreach { value =>
      countValues(AttributeType.STRING, data, "=", value) shouldBe 1
    }
    countValues(AttributeType.STRING, Seq("abc", "ab", "bc", null), "contains", "ab") shouldBe 2
  }

  it should "compare large long values without losing precision" in {
    countValues(
      AttributeType.LONG,
      Seq(Long.MaxValue, Long.MaxValue - 1),
      "=",
      Long.MaxValue.toString
    ) shouldBe 1
    countValues(AttributeType.INTEGER, Seq(-1, 0, 1), ">", "0.5") shouldBe 1
    countValues(AttributeType.DOUBLE, Seq(-0.0, 0.0, 1.0), "=", "0") shouldBe 2
  }

  it should "support boolean equality and timestamp comparisons" in {
    countValues(AttributeType.BOOLEAN, Seq(true, false, null), "=", "true") shouldBe 1
    countValues(
      AttributeType.TIMESTAMP,
      Seq(Timestamp.valueOf("2015-01-01 00:00:00"), Timestamp.valueOf("2015-01-02 00:00:00"), null),
      ">",
      "2015-01-01 00:00:00"
    ) shouldBe 1
  }

  it should "reject non-finite numeric input when evaluating a condition" in {
    Seq(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).foreach { value =>
      intercept[IllegalArgumentException] {
        countValues(AttributeType.DOUBLE, Seq(value), ">", "0")
      }.getMessage should include("finite numbers")
    }
  }

  it should "normalize null group keys without changing the logical descriptor" in {
    val desc = descriptor(Seq(measure("n")))
    desc.groupByKeys = null
    run(desc, Seq(rows)).head.getFields.toSeq shouldBe Seq(6)
    desc.groupByKeys shouldBe null
  }

  it should "round-trip conditions and expose them as optional generated form properties" in {
    val desc = descriptor(Seq(measure("n", Seq(rule("sex", "=", "F")))))
    val restored =
      objectMapper.readValue(objectMapper.writeValueAsString(desc), classOf[AggregateOpDesc])
    run(restored, Seq(rows)).find(_.getField[Any]("year") == 2014).get.getField[Int]("n") shouldBe 0
    val definition = OperatorMetadataGenerator
      .generateOperatorJsonSchema(classOf[AggregateOpDesc])
      .path("definitions")
      .path("AggregationOperation")
    definition.path("properties").path("conditions").path("type").asText shouldBe "array"
    definition.path("properties").path("conditions").path("title").asText shouldBe "Conditions"
    definition
      .path("required")
      .elements()
      .asScala
      .map(_.asText())
      .toSet should not contain "conditions"
    definition
      .path("properties")
      .path("conditionMatch")
      .path("enum")
      .elements()
      .asScala
      .map(_.asText())
      .toSet shouldBe Set("all", "any")
  }

  it should "validate every rule even when another rule would short-circuit" in {
    val bad = measure("n", Seq(rule("sex", "is not null"), rule("absent", "=", "x")), mode = "any")
    intercept[IllegalArgumentException](validate(bad)).getMessage should include("Condition column")
    val executor = new AggregateOpExec(objectMapper.writeValueAsString(descriptor(Seq(bad))))
    executor.open()
    try {
      intercept[IllegalArgumentException](
        executor.processTuple(rows.head, 0)
      ).getMessage should include("Condition column")
    } finally executor.close()
  }

  it should "reject null collections, null rules and unsupported condition modes" in {
    Seq[Any](null, Seq(null)).foreach { conditions =>
      intercept[IllegalArgumentException](
        validate(measure("n") + ("conditions" -> conditions))
      ).getMessage should include("Condition")
    }
    Seq[Any](null, "ALL", "xor", 1).foreach { mode =>
      intercept[IllegalArgumentException](
        validate(measure("n") + ("conditionMatch" -> mode))
      ).getMessage should include("Condition match")
    }
  }

  it should "reject missing fields, invalid operators and invalid typed values before execution" in {
    val invalid = Seq(
      rule(null, "=", "F"),
      rule("missing", "=", "F"),
      rule("sex", null, "F"),
      rule("sex", "bogus", "F"),
      rule("sex", "="),
      rule("age", ">", "not-a-number"),
      rule("age", "contains", "1"),
      rule("age", "=", "NaN"),
      rule("age", "=", "Infinity")
    )
    invalid.foreach { condition =>
      intercept[IllegalArgumentException](
        validate(measure("n", Seq(condition)))
      ).getMessage should include("Condition")
    }
    Seq(rule("v", "<", "true"), rule("v", "=", "TRUE")).foreach { condition =>
      intercept[IllegalArgumentException](
        validate(measure("n", Seq(condition)), Schema().add("v", AttributeType.BOOLEAN))
      ).getMessage should include("Condition")
    }
    intercept[IllegalArgumentException](
      validate(measure("n", Seq(rule("v", "=", "x"))), Schema().add("v", AttributeType.BINARY))
    ).getMessage should include("Condition")
  }

  Seq(
    ("count", "", Seq[Any](2, 0, 1, 0)),
    ("sum", "amount", Seq[Any](10, 0, 0, 0)),
    ("average", "amount", Seq[Any](10.0, null, null, null)),
    ("min", "amount", Seq[Any](10, null, null, null)),
    ("max", "amount", Seq[Any](10, null, null, null)),
    ("concat", "race", Seq[Any]("BLACK/AFRICAN AMERICAN,OTHER", "", "WHITE", ""))
  ).foreach {
    case (function, column, expected) =>
      it should s"support conditional $function while retaining groups with no matching values" in {
        val operation = measure("out", Seq(rule("sex", "=", "F")), function, column)
        val result = run(descriptor(Seq(operation)), Seq(rows.take(3), rows.drop(3), Seq.empty))
          .map(row => row.getField[Any]("year") -> row.getField[Any]("out"))
          .toMap
        result shouldBe Seq[Any](2013, 2014, 2015, null).zip(expected).toMap
        run(descriptor(Seq(operation)), Seq(Seq.empty)) shouldBe empty
        intercept[IllegalArgumentException] {
          validate(operation + ("conditions" -> Seq(rule("age", ">", "invalid"))))
        }.getMessage should include("Condition")
      }
  }

  Seq(AttributeType.INTEGER, AttributeType.LONG, AttributeType.DOUBLE, AttributeType.TIMESTAMP)
    .foreach { kind =>
      it should s"weight conditional and unconditional $kind averages by non-null row counts" in {
        val schema = Schema().add("v", kind).add("selected", AttributeType.BOOLEAN)
        def row(value: Int, selected: Boolean = true): Tuple = {
          val typed: Any = kind match {
            case AttributeType.LONG      => value.toLong
            case AttributeType.DOUBLE    => value.toDouble
            case AttributeType.TIMESTAMP => new Timestamp(value.toLong)
            case _                       => value
          }
          Tuple(schema, Array[Any](typed, selected))
        }
        val partitions = Seq(
          Seq(row(10), row(20), row(30)),
          Seq(row(100)),
          Seq(Tuple(schema, Array[Any](null, true))),
          Seq.empty
        )
        Seq(Seq.empty, Seq(rule("selected", "=", "true"))).foreach { conditions =>
          val desc = descriptor(Seq(measure("avg", conditions, "average", "v")), Seq.empty)
          val data = if (conditions.isEmpty) partitions else partitions :+ Seq(row(1000, false))
          Seq(data, data.reverse, Seq(data.flatten)).foreach { split =>
            run(desc, split, schema).head.getField[Double]("avg") shouldBe 40.0
          }
        }
      }
    }

  Seq(
    (AttributeType.INTEGER, Int.MaxValue, Int.MinValue),
    (AttributeType.LONG, Long.MaxValue, Long.MinValue),
    (AttributeType.DOUBLE, Double.PositiveInfinity, Double.NegativeInfinity),
    (AttributeType.TIMESTAMP, new Timestamp(Long.MaxValue), new Timestamp(-1000L))
  ).foreach {
    case (kind, largest, smallest) =>
      it should s"keep $kind extrema distinct from empty conditional partials" in {
        val schema = Schema().add("v", kind).add("selected", AttributeType.BOOLEAN)
        Seq("min" -> largest, "max" -> smallest).foreach {
          case (function, value) =>
            val desc = descriptor(
              Seq(measure("out", Seq(rule("selected", "=", "true")), function, "v")),
              Seq.empty
            )
            val partitions = Seq(
              Seq(Tuple(schema, Array[Any](value, true))),
              Seq(Tuple(schema, Array[Any](null, true))),
              Seq(Tuple(schema, Array[Any](value, false))),
              Seq.empty
            )
            Seq(partitions, partitions.reverse).foreach { data =>
              run(desc, data, schema).head.getField[Any]("out") shouldBe value
            }
        }
      }
  }

  it should "merge CONCAT without separators from workers with no matches" in {
    val schema = Schema().add("v", AttributeType.STRING).add("selected", AttributeType.BOOLEAN)
    def row(value: String, selected: Boolean = true): Tuple =
      Tuple(schema, Array[Any](value, selected))
    val desc = descriptor(
      Seq(measure("out", Seq(rule("selected", "=", "true")), "concat", "v")),
      Seq.empty
    )
    val partitions = Seq(
      Seq(row(null), row("北"), row(null), row(""), row("東京")),
      Seq(row("excluded", false)),
      Seq(row("tail")),
      Seq(row(null)),
      Seq.empty
    )
    run(desc, partitions, schema).head.getField[String]("out") shouldBe "北,,,東京,tail,"
  }

  it should "preserve matched CONCAT null and empty slots across worker boundaries" in {
    val schema = Schema().add("v", AttributeType.STRING).add("selected", AttributeType.BOOLEAN)
    def row(value: String, selected: Boolean = true): Tuple =
      Tuple(schema, Array[Any](value, selected))

    Seq(Seq.empty, Seq(rule("selected", "=", "true"))).foreach { conditions =>
      val desc = descriptor(Seq(measure("out", conditions, "concat", "v")), Seq.empty)
      Seq(null, "").foreach { blank =>
        val partitions = Seq(Seq(row("text")), Seq(row(blank)))
        run(desc, partitions, schema).head.getField[String]("out") shouldBe "text,"
      }
      val partitions = Seq(
        Seq(row(null), row("")),
        Seq(row("α,"), row(null)),
        Seq(row(""), row("東京")),
        Seq(row(null)),
        Seq.empty
      )
      // CONCAT is order-sensitive: changing worker arrival order may change the
      // text, but splitting the same ordered rows must not drop matched slots.
      partitions.permutations.foreach { ordered =>
        val single = run(desc, Seq(ordered.flatten), schema).head.getField[String]("out")
        run(desc, ordered, schema).head.getField[String]("out") shouldBe single
      }
      run(desc, Seq(Seq(row(null)), Seq(row(""))), schema).head
        .getField[String]("out") shouldBe ""
    }

    val conditional = descriptor(
      Seq(measure("out", Seq(rule("selected", "=", "true")), "concat", "v")),
      Seq.empty
    )
    run(
      conditional,
      Seq(Seq(row("text")), Seq(row(null, false), row("", false)), Seq(row(null))),
      schema
    ).head.getField[String]("out") shouldBe "text,"
  }

  it should "support any matching independently for every aggregation" in {
    val functions = Seq("count", "sum", "average", "min", "max", "concat")
    val desc = descriptor(
      functions.map { function =>
        measure(
          function,
          Seq(rule("sex", "=", "M"), rule("age", ">=", "49")),
          function,
          if (function == "concat") "race" else "amount",
          "any"
        )
      },
      Seq.empty
    )
    val result = run(desc, Seq(rows.take(2), rows.drop(2))).head
    result.getFields.toSeq shouldBe Seq(2, 50, 25.0, 20, 30, "WHITE,BLACK,WHITE")
  }
}
