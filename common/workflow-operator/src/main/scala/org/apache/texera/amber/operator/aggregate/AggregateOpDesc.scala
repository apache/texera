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
import org.apache.texera.amber.core.state.NoLoopVariable
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator, StandaloneHelpers}
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
  @NoLoopVariable
  var aggregations: List[AggregationOperation] = List()

  @JsonProperty("groupByKeys")
  @JsonSchemaTitle("Group By Keys")
  @JsonPropertyDescription("group by columns")
  @AutofillAttributeNameList
  @NoLoopVariable
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
    // The global stage aggregates the local stage's results, so its desc names
    // them. The operator keeps what the user set: the export reads it after
    // compiling, and a second compile would apply getFinal twice.
    val userAggregations = aggregations
    aggregations = aggregations.map(aggr => aggr.getFinal)
    val finalDesc = objectMapper.writeValueAsString(this)
    aggregations = userAggregations

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

  override def standaloneHelpers(): Seq[String] =
    Seq(StandaloneHelpers.AttributeCasts, AggregateOpDesc.Helpers)

  private def build(declaredType: String => Option[AttributeType]): String = {
    val keys = Option(groupByKeys).getOrElse(List())
    val aggs = Option(aggregations).getOrElse(List())

    if (keys.isEmpty) {
      val rowEntries = aggs
        .map(agg =>
          s"    ${pyStringLiteral(agg.resultAttribute)}: ${aggExprScalar(agg, declaredType)},"
        )
        .mkString("\n")
      val resultsLit =
        aggs.map(agg => pyStringLiteral(agg.resultAttribute)).mkString("[", ", ", "]")
      // The engine opens a group when a row arrives, so with no rows there is no
      // group to finish and it emits nothing, not one row of COUNT 0.
      s"""if len(in1df):
         |    out1df = pd.DataFrame([{
         |${rowEntries.linesIterator.map("    " + _).mkString("\n")}
         |    }])
         |else:
         |    out1df = pd.DataFrame(columns=$resultsLit)""".stripMargin
    } else {
      val keysLit = keys.map(pyStringLiteral).mkString("[", ", ", "]")
      // Grouped and joined back by the id of each row's group, never by the
      // key values, which would pair NaN keys again.
      val joinLines = aggs
        .map(agg =>
          s"out1df = out1df.join(${aggExprGroupby(agg, "_texera_agg_groups", declaredType)})"
        )
        .mkString("\n")
      s"""[_texera_agg_ids] = _texera_key_codes(in1df[$keysLit])
         |_texera_agg_groups = in1df.groupby(_texera_agg_ids, sort=False)
         |_texera_agg_first = ~_texera_agg_ids.duplicated()
         |out1df = in1df.loc[_texera_agg_first, $keysLit].set_axis(_texera_agg_ids[_texera_agg_first])
         |$joinLines
         |out1df = out1df.reset_index(drop=True)""".stripMargin
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
          case Some(AttributeType.DOUBLE)    => s"_texera_agg_double_sum(in1df[$attrLit])"
          case _                             => s"in1df[$attrLit].sum()"
        }
      case AggregationFunction.AVERAGE =>
        declared match {
          case Some(AttributeType.TIMESTAMP) => s"_texera_agg_ts_mean(in1df[$attrLit])"
          case Some(AttributeType.DOUBLE)    => s"_texera_agg_double_mean(in1df[$attrLit])"
          case _                             => s"in1df[$attrLit].mean()"
        }
      case AggregationFunction.MIN =>
        declared match {
          case Some(AttributeType.DOUBLE)    => s"_texera_agg_double_min(in1df[$attrLit])"
          case Some(AttributeType.TIMESTAMP) => s"_texera_agg_ts_min(in1df[$attrLit])"
          case _                             => s"in1df[$attrLit].min()"
        }
      case AggregationFunction.MAX =>
        declared match {
          case Some(AttributeType.DOUBLE)    => s"_texera_agg_double_max(in1df[$attrLit])"
          case Some(AttributeType.TIMESTAMP) => s"_texera_agg_ts_max(in1df[$attrLit])"
          case _                             => s"in1df[$attrLit].max()"
        }
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
    def byDoubleFold(fold: String): String =
      s"_texera_agg_double_by_group($groups[$attrLit], $fold).rename($resultLit)"
    agg.aggFunction match {
      case AggregationFunction.SUM =>
        declared match {
          case Some(AttributeType.INTEGER) =>
            s"$groups[$attrLit].apply(_texera_agg_int_sum).rename($resultLit)"
          case Some(AttributeType.TIMESTAMP) =>
            s"$groups[$attrLit].apply(_texera_agg_ts_sum).rename($resultLit)"
          case Some(AttributeType.DOUBLE) => byDoubleFold("_texera_agg_double_sum")
          case _                          => s"$groups[$attrLit].sum().rename($resultLit)"
        }
      case AggregationFunction.AVERAGE =>
        declared match {
          case Some(AttributeType.TIMESTAMP) =>
            s"$groups[$attrLit].apply(_texera_agg_ts_mean).rename($resultLit)"
          case Some(AttributeType.DOUBLE) => byDoubleFold("_texera_agg_double_mean")
          case _                          => s"$groups[$attrLit].mean().rename($resultLit)"
        }
      case AggregationFunction.MIN =>
        declared match {
          case Some(AttributeType.DOUBLE) => byDoubleFold("_texera_agg_double_min")
          case Some(AttributeType.TIMESTAMP) =>
            s"$groups[$attrLit].apply(_texera_agg_ts_min).rename($resultLit)"
          case _ => s"$groups[$attrLit].min().rename($resultLit)"
        }
      case AggregationFunction.MAX =>
        declared match {
          case Some(AttributeType.DOUBLE) => byDoubleFold("_texera_agg_double_max")
          case Some(AttributeType.TIMESTAMP) =>
            s"$groups[$attrLit].apply(_texera_agg_ts_max).rename($resultLit)"
          case _ => s"$groups[$attrLit].max().rename($resultLit)"
        }
      case AggregationFunction.COUNT =>
        if (agg.attribute == null || agg.attribute.isEmpty)
          s"$groups.size().rename($resultLit)"
        else s"$groups[$attrLit].count().rename($resultLit)"
      case AggregationFunction.CONCAT =>
        s"$groups[$attrLit].apply(_texera_agg_concat).rename($resultLit)"
    }
  }
}

object AggregateOpDesc {

  /**
    * The Python folds Aggregate's standalone code calls, where pandas' own would
    * answer differently from the engine.
    *
    *  - `_texera_agg_concat`: concatAgg, a comma only after the first non-empty value.
    *  - `_texera_agg_int_sum`: an INTEGER sum, wrapping as a Java int.
    *  - `_texera_agg_ts_epoch_ms`: getTime as epoch milliseconds, by `_texera_wall_clock_instants`
    *    from StandaloneHelpers.AttributeCasts.
    *  - `_texera_agg_ts_sum`: a timestamp SUM, wrapping as a Java long.
    *  - `_texera_agg_ts_mean`: a timestamp AVERAGE, a DOUBLE of epoch milliseconds.
    *  - `_texera_agg_ts_pick`, `_min`, `_max`: by getTime, keeping the first of a tie.
    *  - `_texera_agg_double_*`: DOUBLE folds that keep a stored NaN apart from null,
    *    ordered by `_texera_double_key` from StandaloneHelpers.AttributeCasts.
    *
    * Groups are told apart by `_texera_key_codes` from StandaloneHelpers.AttributeCasts.
    */
  val Helpers: String =
    """def _texera_agg_concat(series):
      |    partial = ""
      |    for v, missing in zip(series, series.isna()):
      |        text = "" if missing else _texera_java_text(v)
      |        partial = text if partial == "" else partial + "," + text
      |    return partial
      |
      |def _texera_agg_int_sum(series):
      |    total = int(series.sum())
      |    return ((total + (1 << 31)) % (1 << 32)) - (1 << 31)
      |
      |def _texera_agg_ts_epoch_ms(series):
      |    return _texera_wall_clock_instants(series.dropna()).astype("int64")
      |
      |def _texera_agg_ts_sum(series):
      |    total = int(_texera_agg_ts_epoch_ms(series).astype(object).sum())
      |    total = ((total + (1 << 63)) % (1 << 64)) - (1 << 63)
      |    import datetime
      |
      |    utc = pd.Timestamp(total, unit="ms")
      |    zone = _texera_local_zone()
      |    try:
      |        moment = utc.to_pydatetime().replace(tzinfo=datetime.timezone.utc)
      |        shift = _texera_local_wall_clock(moment, zone) - moment.replace(tzinfo=None)
      |    except (OSError, OverflowError):
      |        shift = _texera_standard_offset(zone)
      |    return utc + shift
      |
      |def _texera_agg_ts_mean(series):
      |    kept = _texera_agg_ts_epoch_ms(series)
      |    if len(kept) == 0:
      |        return None
      |    return float(kept.astype(object).sum()) / len(kept)
      |
      |def _texera_agg_ts_pick(series, better):
      |    kept = series.dropna()
      |    if len(kept) == 0:
      |        return pd.NaT
      |    millis = kept.dt.floor("ms")
      |    pick = 0
      |    for i in range(1, len(kept)):
      |        if better(millis.iloc[i], millis.iloc[pick]):
      |            pick = i
      |    return kept.iloc[pick]
      |
      |def _texera_agg_ts_min(series):
      |    return _texera_agg_ts_pick(series, lambda a, b: a < b)
      |
      |def _texera_agg_ts_max(series):
      |    return _texera_agg_ts_pick(series, lambda a, b: a > b)
      |
      |def _texera_agg_double_values(series):
      |    return [float(v) for v in series.dropna()]
      |
      |def _texera_agg_double_sum(series):
      |    partial = 0.0
      |    for v in _texera_agg_double_values(series):
      |        partial += v
      |    return partial
      |
      |def _texera_agg_double_mean(series):
      |    kept = _texera_agg_double_values(series)
      |    if not kept:
      |        return pd.NA
      |    return _texera_agg_double_sum(series) / len(kept)
      |
      |def _texera_agg_double_min(series):
      |    start = 1.7976931348623157e308
      |    partial = start
      |    for v in _texera_agg_double_values(series):
      |        if _texera_double_key(v) < _texera_double_key(partial):
      |            partial = v
      |    return pd.NA if partial == start else partial
      |
      |def _texera_agg_double_max(series):
      |    start = float("-inf")
      |    partial = start
      |    for v in _texera_agg_double_values(series):
      |        if _texera_double_key(v) > _texera_double_key(partial):
      |            partial = v
      |    return pd.NA if partial == start else partial
      |
      |def _texera_agg_double_by_group(column, fold):
      |    results = column.apply(fold)
      |    mask = results.map(lambda v: v is pd.NA).to_numpy(bool)
      |    values = results.where(~mask, 0.0).to_numpy(float)
      |    return pd.Series(pd.arrays.FloatingArray(values, mask), index=results.index)""".stripMargin
}
