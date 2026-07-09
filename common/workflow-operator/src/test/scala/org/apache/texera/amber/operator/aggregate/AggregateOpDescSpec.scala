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

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorMetadataGenerator}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class AggregateOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def aggOp(fn: AggregationFunction, attr: String, result: String): AggregationOperation = {
    val a = new AggregationOperation()
    a.aggFunction = fn
    a.attribute = attr
    a.resultAttribute = result
    a
  }

  // Each test builds a FRESH desc: getPhysicalPlan mutates `aggregations` (getFinal),
  // so the descriptor is intentionally not idempotent across calls.
  private def descWith(keys: List[String], aggs: AggregationOperation*): AggregateOpDesc = {
    val d = new AggregateOpDesc
    d.groupByKeys = keys
    d.aggregations = aggs.toList
    d
  }

  "AggregateOpDesc.operatorInfo" should "advertise the name and Aggregate group" in {
    val info = (new AggregateOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Aggregate"
    info.operatorDescription shouldBe "Calculate different types of aggregation values"
    info.operatorGroupName shouldBe OperatorGroupConstants.AGGREGATE_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe false
  }

  "AggregateOpDesc.getPhysicalPlan" should
    "build a two-stage (localAgg + globalAgg) plan with one connecting link" in {
    val plan = descWith(List("city"), aggOp(AggregationFunction.SUM, "sales", "total"))
      .getPhysicalPlan(workflowId, executionId)
    plan.operators should have size 2
    plan.links should have size 1
  }

  "AggregateOpDesc schema propagation" should
    "produce the group-by keys plus the aggregation result column (SUM keeps the input type)" in {
    val input = Schema().add("city", AttributeType.STRING).add("sales", AttributeType.INTEGER)
    val out = descWith(List("city"), aggOp(AggregationFunction.SUM, "sales", "total"))
      .getExternalOutputSchemas(Map(PortIdentity() -> input))
    out shouldBe Map(
      PortIdentity() -> Schema()
        .add("city", AttributeType.STRING)
        .add("total", AttributeType.INTEGER)
    )
  }

  it should "type a COUNT result as INTEGER and an AVERAGE result as DOUBLE" in {
    val input = Schema().add("v", AttributeType.LONG)
    descWith(List.empty, aggOp(AggregationFunction.COUNT, "v", "cnt"))
      .getExternalOutputSchemas(Map(PortIdentity() -> input)) shouldBe
      Map(PortIdentity() -> Schema().add("cnt", AttributeType.INTEGER))
    descWith(List.empty, aggOp(AggregationFunction.AVERAGE, "v", "avg"))
      .getExternalOutputSchemas(Map(PortIdentity() -> input)) shouldBe
      Map(PortIdentity() -> Schema().add("avg", AttributeType.DOUBLE))
  }

  it should "type a COUNT(*) (empty attribute) result as INTEGER without looking up an input column" in {
    // An empty attribute means COUNT(*); schema propagation must not dereference a column.
    val input = Schema().add("v", AttributeType.LONG)
    descWith(List.empty, aggOp(AggregationFunction.COUNT, "", "row_count"))
      .getExternalOutputSchemas(Map(PortIdentity() -> input)) shouldBe
      Map(PortIdentity() -> Schema().add("row_count", AttributeType.INTEGER))
  }

  it should "fail fast for a non-COUNT function with an empty attribute (only COUNT allows it)" in {
    // Only COUNT tolerates a blank attribute; SUM/etc. must resolve the column and fail
    // fast rather than propagate a null-typed output.
    val input = Schema().add("v", AttributeType.LONG)
    assertThrows[Exception] {
      descWith(List.empty, aggOp(AggregationFunction.SUM, "", "total"))
        .getExternalOutputSchemas(Map(PortIdentity() -> input))
    }
  }

  "AggregateOpDesc JSON schema" should
    "make the attribute optional only for count and required for every other function" in {
    val aggDef = OperatorMetadataGenerator
      .generateOperatorJsonSchema(classOf[AggregateOpDesc])
      .get("definitions")
      .get("AggregationOperation")

    // attribute is not unconditionally required (aggFunction still is)
    val baseRequired = aggDef.get("required").elements().asScala.map(_.asText()).toSet
    baseRequired should contain("aggFunction")
    baseRequired should not contain "attribute"

    // conditional rule: count -> no attribute requirement; any other function -> attribute required
    val rule = aggDef
      .get("allOf")
      .elements()
      .asScala
      .find(node => node.has("if") && node.has("else"))
      .getOrElse(fail("expected a conditional if/else rule in the AggregationOperation schema"))
    rule.get("if").get("properties").get("aggFunction").get("const").asText() shouldBe "count"
    val elseRequired = rule.get("else").get("required").elements().asScala.map(_.asText()).toList
    elseRequired should contain("attribute")
  }
}
