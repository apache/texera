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

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeNameList
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

import javax.validation.constraints.{NotNull, Size}

class AggregateOpDesc extends LogicalOp with StandaloneCodeGenerator {

  @JsonProperty(value = "aggregations", required = true)
  @JsonPropertyDescription("multiple aggregation functions")
  @NotNull(message = "aggregation cannot be null")
  @Size(min = 1, message = "aggregations cannot be empty")
  var aggregations: List[AggregationOperation] = List()

  @JsonProperty("groupByKeys")
  @JsonSchemaTitle("Group By Keys")
  @JsonPropertyDescription("group by columns")
  @AutofillAttributeNameList
  var groupByKeys: List[String] = List()

  override def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan = {
    if (groupByKeys == null) groupByKeys = List()
    // TODO: this is supposed to be blocking but due to limitations of materialization naming on the logical operator
    // we are keeping it not annotated as blocking.
    val inputPort = InputPort(PortIdentity())
    val outputPort = OutputPort(PortIdentity(internal = true))
    val partialDesc = objectMapper.writeValueAsString(this)
    val localAggregations = List(aggregations: _*)
    val partialPhysicalOp = PhysicalOp
      .oneToOnePhysicalOp(
        PhysicalOpIdentity(operatorIdentifier, "localAgg"),
        workflowId,
        executionId,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.aggregate.AggregateOpExec",
          partialDesc
        )
      )
      .withIsOneToManyOp(true)
      .withInputPorts(List(inputPort))
      .withOutputPorts(List(outputPort))
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          val inputSchema = inputSchemas(operatorInfo.inputPorts.head.id)
          val outputSchema = Schema(
            groupByKeys.map(key => inputSchema.getAttribute(key)) ++
              localAggregations.map { agg =>
                // Only COUNT with an empty attribute (COUNT(*)) skips the column lookup:
                // its result type is INTEGER regardless. Every other function resolves
                // the input attribute (failing fast if it is missing/invalid).
                val attrType =
                  if (
                    agg.aggFunction == AggregationFunction.COUNT &&
                    (agg.attribute == null || agg.attribute.trim.isEmpty)
                  ) null
                  else inputSchema.getAttribute(agg.attribute).getType
                agg.getAggregationAttribute(attrType)
              }
          )
          Map(PortIdentity(internal = true) -> outputSchema)
        })
      )

    val finalInputPort = InputPort(PortIdentity(0, internal = true))
    val finalOutputPort = OutputPort(PortIdentity(0), blocking = true)
    // change aggregations to final
    aggregations = aggregations.map(aggr => aggr.getFinal)
    val finalDesc = objectMapper.writeValueAsString(this)

    val finalPhysicalOp = PhysicalOp
      .oneToOnePhysicalOp(
        PhysicalOpIdentity(operatorIdentifier, "globalAgg"),
        workflowId,
        executionId,
        OpExecWithClassName("org.apache.texera.amber.operator.aggregate.AggregateOpExec", finalDesc)
      )
      .withParallelizable(false)
      .withIsOneToManyOp(true)
      .withInputPorts(List(finalInputPort))
      .withOutputPorts(List(finalOutputPort))
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas =>
          Map(operatorInfo.outputPorts.head.id -> inputSchemas(finalInputPort.id))
        )
      )
      .withPartitionRequirement(List(Option(HashPartition(groupByKeys))))
      .withDerivePartition(_ => HashPartition(groupByKeys))

    var plan = PhysicalPlan(
      operators = Set(partialPhysicalOp, finalPhysicalOp),
      links = Set(
        PhysicalLink(partialPhysicalOp.id, outputPort.id, finalPhysicalOp.id, finalInputPort.id)
      )
    )
    plan.operators.foreach(op => plan = plan.setOperator(op.withIsOneToManyOp(true)))
    plan
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Aggregate",
      "Calculate different types of aggregation values",
      OperatorGroupConstants.AGGREGATE_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  // The engine aggregates in two phases across partitions; one process needs
  // only the one groupby, or a single-row reduction when no key is grouped on.
  //
  // Must run before `getPhysicalPlan`, which rewrites `aggregations` in place:
  // it turns COUNT into SUM for the final phase, and this reads them as written.
  override def generateStandaloneCode(): String = {
    val keys = Option(groupByKeys).getOrElse(List())
    val aggs = Option(aggregations).getOrElse(List())

    // Identical helper definition each call — keeps the standalone module
    // self-contained without relying on a shared prelude.
    val concatHelper =
      """def _texera_agg_concat(series):
        |    parts = []
        |    started = False
        |    for v in series:
        |        if not started:
        |            if pd.isna(v):
        |                continue
        |            parts.append(str(v))
        |            started = True
        |        else:
        |            parts.append("" if pd.isna(v) else str(v))
        |    return ",".join(parts)""".stripMargin

    if (keys.isEmpty) {
      val rowEntries = aggs
        .map(agg => s"    ${pyStringLiteral(agg.resultAttribute)}: ${aggExprScalar(agg)},")
        .mkString("\n")
      s"""$concatHelper
         |out1df = pd.DataFrame([{
         |$rowEntries
         |}])""".stripMargin
    } else {
      val keysLit = keys.map(pyStringLiteral).mkString("[", ", ", "]")
      val aggLines = aggs.zipWithIndex
        .map {
          case (agg, i) =>
            s"_texera_agg_s$i = ${aggExprGroupby(agg, "_texera_agg_groups")}"
        }
        .mkString("\n")
      val mergeLines = aggs.indices
        .map(i =>
          s"""out1df = out1df.merge(_texera_agg_s$i.reset_index(), on=$keysLit, how="left")"""
        )
        .mkString("\n")
      s"""$concatHelper
         |_texera_agg_groups = in1df.groupby($keysLit, dropna=False, sort=False)
         |out1df = in1df[$keysLit].drop_duplicates().reset_index(drop=True)
         |$aggLines
         |$mergeLines""".stripMargin
    }
  }

  private def aggExprScalar(agg: AggregationOperation): String = {
    val attrLit =
      if (agg.attribute == null || agg.attribute.isEmpty) "None"
      else pyStringLiteral(agg.attribute)
    agg.aggFunction match {
      case AggregationFunction.SUM     => s"in1df[$attrLit].sum()"
      case AggregationFunction.AVERAGE => s"in1df[$attrLit].mean()"
      case AggregationFunction.MIN     => s"in1df[$attrLit].min()"
      case AggregationFunction.MAX     => s"in1df[$attrLit].max()"
      case AggregationFunction.COUNT =>
        if (agg.attribute == null || agg.attribute.isEmpty) "int(len(in1df))"
        else s"int(in1df[$attrLit].count())"
      case AggregationFunction.CONCAT => s"_texera_agg_concat(in1df[$attrLit])"
    }
  }

  private def aggExprGroupby(agg: AggregationOperation, groups: String): String = {
    val attrLit =
      if (agg.attribute == null || agg.attribute.isEmpty) "None"
      else pyStringLiteral(agg.attribute)
    val resultLit = pyStringLiteral(agg.resultAttribute)
    agg.aggFunction match {
      case AggregationFunction.SUM     => s"$groups[$attrLit].sum().rename($resultLit)"
      case AggregationFunction.AVERAGE => s"$groups[$attrLit].mean().rename($resultLit)"
      case AggregationFunction.MIN     => s"$groups[$attrLit].min().rename($resultLit)"
      case AggregationFunction.MAX     => s"$groups[$attrLit].max().rename($resultLit)"
      case AggregationFunction.COUNT =>
        if (agg.attribute == null || agg.attribute.isEmpty)
          s"$groups.size().rename($resultLit)"
        else s"$groups[$attrLit].count().rename($resultLit)"
      case AggregationFunction.CONCAT =>
        s"$groups[$attrLit].apply(_texera_agg_concat).rename($resultLit)"
    }
  }
}
