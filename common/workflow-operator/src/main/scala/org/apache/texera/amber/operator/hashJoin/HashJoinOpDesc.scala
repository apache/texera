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

package org.apache.texera.amber.operator.hashJoin

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.hashJoin.HashJoinOpDesc.HASH_JOIN_INTERNAL_KEY_NAME
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameOnPort1
}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

object HashJoinOpDesc {
  val HASH_JOIN_INTERNAL_KEY_NAME = "__internal__hashtable__key__"
}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "buildAttributeName": {
      "const": {
        "$data": "probeAttributeName"
      }
    }
  }
}
""")
class HashJoinOpDesc[K] extends LogicalOp with StandaloneCodeGenerator {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Left Input Attribute")
  @JsonPropertyDescription("attribute to be joined on the Left Input")
  @AutofillAttributeName
  var buildAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Right Input Attribute")
  @JsonPropertyDescription("attribute to be joined on the Right Input")
  @AutofillAttributeNameOnPort1
  var probeAttributeName: String = _

  @JsonProperty(required = true, defaultValue = "inner")
  @JsonSchemaTitle("Join Type")
  @JsonPropertyDescription("select the join type to execute")
  var joinType: JoinType = JoinType.INNER

  override def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan = {

    val buildInputPort = operatorInfo.inputPorts.head
    val buildOutputPort = OutputPort(PortIdentity(0, internal = true), blocking = true)

    val buildPhysicalOp =
      PhysicalOp
        .oneToOnePhysicalOp(
          PhysicalOpIdentity(operatorIdentifier, "build"),
          workflowId,
          executionId,
          OpExecWithClassName(
            "org.apache.texera.amber.operator.hashJoin.HashJoinBuildOpExec",
            objectMapper.writeValueAsString(this)
          )
        )
        .withInputPorts(List(buildInputPort))
        .withOutputPorts(List(buildOutputPort))
        .withPartitionRequirement(List(Option(HashPartition(List(buildAttributeName)))))
        .withPropagateSchema(
          SchemaPropagationFunc(inputSchemas =>
            Map(
              PortIdentity(internal = true) -> Schema(
                List(
                  new Attribute(
                    HASH_JOIN_INTERNAL_KEY_NAME,
                    // Because we need to materialize the outputs of build, we cannot use ANY type.
                    inputSchemas(operatorInfo.inputPorts.head.id)
                      .getAttribute(buildAttributeName)
                      .getType
                  )
                )
              ).add(inputSchemas(operatorInfo.inputPorts.head.id))
            )
          )
        )
        .withParallelizable(true)

    val probeBuildInputPort = InputPort(PortIdentity(0, internal = true))
    val probeDataInputPort =
      InputPort(operatorInfo.inputPorts(1).id, dependencies = List(probeBuildInputPort.id))
    val probeOutputPort = OutputPort(PortIdentity(0))

    val probePhysicalOp =
      PhysicalOp
        .oneToOnePhysicalOp(
          PhysicalOpIdentity(operatorIdentifier, "probe"),
          workflowId,
          executionId,
          OpExecWithClassName(
            "org.apache.texera.amber.operator.hashJoin.HashJoinProbeOpExec",
            objectMapper.writeValueAsString(this)
          )
        )
        .withInputPorts(
          List(
            probeBuildInputPort,
            probeDataInputPort
          )
        )
        .withOutputPorts(List(probeOutputPort))
        .withPartitionRequirement(
          List(
            // Cannot use OneToOnePartition because it does not work with InputPortMaterializationReaderThreads.
            Option(HashPartition(List(buildAttributeName))),
            Option(HashPartition(List(probeAttributeName)))
          )
        )
        .withDerivePartition(_ => HashPartition(List(probeAttributeName)))
        .withParallelizable(true)
        .withPropagateSchema(
          SchemaPropagationFunc(inputSchemas => {
            val buildSchema = inputSchemas(PortIdentity(internal = true))
            val probeSchema = inputSchemas(PortIdentity(1))

            // Start with the attributes from the build schema, excluding the hash join internal key
            val leftAttributes =
              buildSchema.getAttributes.filterNot(_.getName == HASH_JOIN_INTERNAL_KEY_NAME)
            val leftAttributeNames = leftAttributes.map(_.getName).toSet

            // Filter and rename attributes from the probe schema to avoid conflicts
            val rightAttributes = probeSchema.getAttributes
              .filterNot(_.getName == probeAttributeName)
              .map { attr =>
                var newName = attr.getName
                while (leftAttributeNames.contains(newName)) {
                  val suffixIndex = """#@(\d+)$""".r
                    .findFirstMatchIn(newName)
                    .map(_.group(1).toInt + 1)
                    .getOrElse(1)
                  newName = s"${attr.getName}#@$suffixIndex"
                }
                new Attribute(newName, attr.getType)
              }

            // Combine left and right attributes into a new schema
            val outputSchema = Schema(leftAttributes ++ rightAttributes)
            Map(PortIdentity() -> outputSchema)
          })
        )

    PhysicalPlan(
      operators = Set(buildPhysicalOp, probePhysicalOp),
      links = Set(
        PhysicalLink(
          buildPhysicalOp.id,
          buildOutputPort.id,
          probePhysicalOp.id,
          probeBuildInputPort.id
        )
      )
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hash Join",
      "join two inputs",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(0), displayName = "left"),
        InputPort(PortIdentity(1), displayName = "right", dependencies = List(PortIdentity(0)))
      ),
      outputPorts = List(OutputPort())
    )

  // Equi-join: drop the probe key (kept only when its name differs from the
  // build key), suffix colliding right columns "#@1" — matches JoinUtils. Known
  // Texera divergences: row order, null keys (NaN != NaN in merge), outer
  // anti-row column placement.
  /** Only the declared type can say which columns an outer join widened, so
    * without a schema the widening stands.
    */
  override def generateStandaloneCode(inputSchemas: Map[PortIdentity, Schema]): String = {
    val integral = (a: Attribute) =>
      a.getType == AttributeType.INTEGER || a.getType == AttributeType.LONG
    val declaredIntegers = (port: PortIdentity) =>
      inputSchemas.get(port).map(_.getAttributes.filter(integral).map(_.getName)).getOrElse(List())
    // Each side keeps its own list, under the names that side's frame carries,
    // so a column the merge renames is still the column its own schema typed.
    if (joinType == JoinType.INNER) generateStandaloneCode()
    else
      generateStandaloneCode(
        declaredIntegers(operatorInfo.inputPorts.head.id),
        declaredIntegers(operatorInfo.inputPorts.last.id)
      )
  }

  override def generateStandaloneCode(): String =
    generateStandaloneCode(List(), List())

  private def generateStandaloneCode(
      leftIntegerColumns: List[String],
      rightIntegerColumns: List[String]
  ): String = {
    val buildKeyLit = objectMapper.writeValueAsString(buildAttributeName)
    val probeKeyLit = objectMapper.writeValueAsString(probeAttributeName)
    val how = joinType match {
      case JoinType.INNER       => "inner"
      case JoinType.LEFT_OUTER  => "left"
      case JoinType.RIGHT_OUTER => "right"
      case JoinType.FULL_OUTER  => "outer"
    }
    // An unmatched row leaves a hole, and a hole costs a pandas integer column
    // its type: int64 becomes float64, which rounds every value past 2^53
    // before anything can put the type back. The engine writes a null and
    // leaves the column INTEGER, so widen to the integer dtype that holds a
    // hole before the merge digs one.
    val widen = leftIntegerColumns.nonEmpty || rightIntegerColumns.nonEmpty
    val namesLit = (names: List[String]) => names.map(pyStringLiteral).mkString("[", ", ", "]")
    val widening =
      if (!widen) ""
      else
        s"""_left_ints = {_c: "Int64" for _c in ${namesLit(leftIntegerColumns)} if _c in in1df.columns}
           |_right_ints = {_c: "Int64" for _c in ${namesLit(rightIntegerColumns)} if _c in in2df.columns}
           |""".stripMargin
    val leftFrame = if (widen) "in1df.astype(_left_ints)" else "in1df"
    // Cast before the rename, because the names above are the ones the right
    // input declared.
    val rightFrame =
      if (widen) "in2df.astype(_right_ints).rename(columns=_rename)"
      else "in2df.rename(columns=_rename)"
    // HashJoinProbeOpExec's rename, written out: append "#@1" until the name is
    // free. pandas' `suffixes` appends once and then refuses the duplicate it
    // just made.
    val merge =
      s"""_left_cols = set(in1df.columns)
         |_right_cols = list(in2df.columns)
         |_right_set = set(_right_cols)
         |_rename = {}
         |for _col in _right_cols:
         |    _new = _col
         |    _others = _right_set - {_col}
         |    while _new in _left_cols or _new in _others:
         |        _new = _new + "#@1"
         |    _rename[_col] = _new
         |_probe_key = _rename.get($probeKeyLit, $probeKeyLit)
         |""".stripMargin + widening +
        s"""out1df = $leftFrame.merge(
           |    $rightFrame,
           |    how=${pyStringLiteral(how)},
           |    left_on=$buildKeyLit,
           |    right_on=_probe_key,
           |)""".stripMargin
    // `_probe_key` is whatever the rename settled on, so this drops the right
    // column rather than a left one that happened to share its name. Asked of
    // the name the rename produced and not of the two the operator was given:
    // when both sides name the key alike the rename still moves the right one
    // aside, and a test on the operator's own two names read that as one shared
    // column and kept the copy, where the engine emits the key once.
    val tail =
      s"""if _probe_key != $buildKeyLit:
         |    out1df = out1df.drop(columns=[_probe_key])
         |out1df = out1df.reset_index(drop=True)""".stripMargin
    s"$merge\n$tail"
  }
}
