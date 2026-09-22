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
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
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
import scala.util.Try

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

  /** The engine aggregates in two phases across partitions; one process needs
    * only the one groupby, or a single-row reduction when no key is grouped on.
    *
    * Must run before `getPhysicalPlan`, which rewrites `aggregations` in place:
    * it turns COUNT into SUM for the final phase, and this reads them as
    * written.
    *
    * SUM and AVERAGE follow the column's DECLARED type: a holed INTEGER column
    * arrives as a float, and an INTEGER and a LONG arrive alike.
    */
  override def generateStandaloneCode(inputSchemas: Map[PortIdentity, Schema]): String = {
    val schema = inputSchemas.get(operatorInfo.inputPorts.head.id)
    build(name => schema.flatMap(s => Try(s.getAttribute(name).getType).toOption))
  }

  override def generateStandaloneCode(): String = build(_ => None)

  private def build(declaredType: String => Option[AttributeType]): String = {
    val keys = Option(groupByKeys).getOrElse(List())
    val aggs = Option(aggregations).getOrElse(List())

    // Identical helper definition each call — keeps the standalone module
    // self-contained without relying on a shared prelude.
    val concatHelper =
      """def _texera_agg_concat(series):
        |    # The accumulator starts empty and only earns a separator once it
        |    # holds something, so a leading empty value adds neither text nor
        |    # comma: "", "a", "" concatenates to "a," and not ",a,". A null is
        |    # read as the empty string, which is what makes the two the same
        |    # here. This is concatAgg's fold, written out.
        |    partial = ""
        |    for v in series:
        |        if pd.isna(v):
        |            text = ""
        |        elif isinstance(v, bool) or (hasattr(v, "dtype") and v.dtype == bool):
        |            # Java's toString spells a boolean in lower case.
        |            text = "true" if v else "false"
        |        else:
        |            text = str(v)
        |        partial = text if partial == "" else partial + "," + text
        |    return partial
        |
        |def _texera_agg_int_sum(series):
        |    # The engine adds an INTEGER column as Java ints, which wrap.
        |    total = int(series.sum())
        |    return ((total + (1 << 31)) % (1 << 32)) - (1 << 31)
        |
        |def _texera_agg_ts_zone():
        |    # The engine reads and writes a timestamp in the JVM's default zone,
        |    # so the arithmetic below has to name the same one. gettz() and not
        |    # the current offset: the zone carries its daylight rules, and each
        |    # instant needs the offset in force when it happened.
        |    from dateutil.tz import gettz
        |
        |    return gettz()
        |
        |def _texera_agg_ts_epoch_ms(series):
        |    # A timestamp reaches the engine as its epoch milliseconds whatever
        |    # resolution the column carries, and reading the integers out of a
        |    # microsecond column asks for a different number than a nanosecond
        |    # one, so cast to milliseconds before reading them.
        |    #
        |    # A column holds a wall clock, and `Timestamp.getTime` answers for
        |    # the instant that wall clock names locally, so localize before
        |    # reading the integers out: left as UTC every one of them is a whole
        |    # offset away. The two flags are java.time's own reading of an hour
        |    # daylight saving repeats (the later one) or skips (shifted past the
        |    # gap).
        |    return (
        |        series.dropna()
        |        .astype("datetime64[ms]")
        |        .dt.tz_localize(
        |            _texera_agg_ts_zone(), ambiguous=False, nonexistent=pd.Timedelta("1h")
        |        )
        |        .astype("int64")
        |    )
        |
        |def _texera_agg_ts_sum(series):
        |    # SUM keeps the column's own type, so the engine adds the epoch
        |    # milliseconds as Java longs, which wrap, and builds a timestamp
        |    # from the total. Python integers carry the sum exactly, so the
        |    # wrap is the only place a total loses anything.
        |    total = int(_texera_agg_ts_epoch_ms(series).astype(object).sum())
        |    total = ((total + (1 << 63)) % (1 << 64)) - (1 << 63)
        |    return (
        |        pd.Timestamp(total, unit="ms", tz="UTC")
        |        .tz_convert(_texera_agg_ts_zone())
        |        .tz_localize(None)
        |    )
        |
        |def _texera_agg_ts_mean(series):
        |    # AVERAGE is declared DOUBLE whatever column it reads, so this is
        |    # the mean of the epoch milliseconds and not a timestamp.
        |    kept = _texera_agg_ts_epoch_ms(series)
        |    if len(kept) == 0:
        |        return None
        |    return float(kept.astype(object).sum()) / len(kept)""".stripMargin

    if (keys.isEmpty) {
      val rowEntries = aggs
        .map(agg =>
          s"    ${pyStringLiteral(agg.resultAttribute)}: ${aggExprScalar(agg, declaredType)},"
        )
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
            s"_texera_agg_s$i = ${aggExprGroupby(agg, "_texera_agg_groups", declaredType)}"
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

  private def aggExprScalar(
      agg: AggregationOperation,
      declaredType: String => Option[AttributeType]
  ): String = {
    val attrLit =
      if (agg.attribute == null || agg.attribute.isEmpty) "None"
      else pyStringLiteral(agg.attribute)
    val declared = Option(agg.attribute).filter(_.nonEmpty).flatMap(declaredType)
    agg.aggFunction match {
      case AggregationFunction.SUM =>
        declared match {
          case Some(AttributeType.INTEGER)   => s"_texera_agg_int_sum(in1df[$attrLit])"
          case Some(AttributeType.TIMESTAMP) => s"_texera_agg_ts_sum(in1df[$attrLit])"
          case _                             => s"in1df[$attrLit].sum()"
        }
      case AggregationFunction.AVERAGE =>
        declared match {
          case Some(AttributeType.TIMESTAMP) => s"_texera_agg_ts_mean(in1df[$attrLit])"
          case _                             => s"in1df[$attrLit].mean()"
        }
      case AggregationFunction.MIN => s"in1df[$attrLit].min()"
      case AggregationFunction.MAX => s"in1df[$attrLit].max()"
      case AggregationFunction.COUNT =>
        if (agg.attribute == null || agg.attribute.isEmpty) "int(len(in1df))"
        else s"int(in1df[$attrLit].count())"
      case AggregationFunction.CONCAT => s"_texera_agg_concat(in1df[$attrLit])"
    }
  }

  private def aggExprGroupby(
      agg: AggregationOperation,
      groups: String,
      declaredType: String => Option[AttributeType]
  ): String = {
    val attrLit =
      if (agg.attribute == null || agg.attribute.isEmpty) "None"
      else pyStringLiteral(agg.attribute)
    val resultLit = pyStringLiteral(agg.resultAttribute)
    val declared = Option(agg.attribute).filter(_.nonEmpty).flatMap(declaredType)
    agg.aggFunction match {
      case AggregationFunction.SUM =>
        declared match {
          case Some(AttributeType.INTEGER) =>
            s"$groups[$attrLit].apply(_texera_agg_int_sum).rename($resultLit)"
          case Some(AttributeType.TIMESTAMP) =>
            s"$groups[$attrLit].apply(_texera_agg_ts_sum).rename($resultLit)"
          case _ => s"$groups[$attrLit].sum().rename($resultLit)"
        }
      case AggregationFunction.AVERAGE =>
        declared match {
          case Some(AttributeType.TIMESTAMP) =>
            s"$groups[$attrLit].apply(_texera_agg_ts_mean).rename($resultLit)"
          case _ => s"$groups[$attrLit].mean().rename($resultLit)"
        }
      case AggregationFunction.MIN => s"$groups[$attrLit].min().rename($resultLit)"
      case AggregationFunction.MAX => s"$groups[$attrLit].max().rename($resultLit)"
      case AggregationFunction.COUNT =>
        if (agg.attribute == null || agg.attribute.isEmpty)
          s"$groups.size().rename($resultLit)"
        else s"$groups[$attrLit].count().rename($resultLit)"
      case AggregationFunction.CONCAT =>
        s"$groups[$attrLit].apply(_texera_agg_concat).rename($resultLit)"
    }
  }
}
