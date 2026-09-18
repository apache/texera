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
  /** An outer join leaves holes, and pandas pays for a hole in an integer column
    * by widening it to float: the engine keeps the column INTEGER and writes a
    * null. Only the declared type can say which columns to put back, so without
    * a schema the widening stands.
    */
  override def generateStandaloneCode(inputSchemas: Map[PortIdentity, Schema]): String = {
    val block = generateStandaloneCode()
    val integerColumns = inputSchemas.values
      .flatMap(_.getAttributes)
      .filter(a => a.getType == AttributeType.INTEGER || a.getType == AttributeType.LONG)
      .map(_.getName)
      .toSeq
      .distinct
    if (integerColumns.isEmpty || joinType == JoinType.INNER) block
    else {
      val namesLit = integerColumns.map(pyStringLiteral).mkString("[", ", ", "]")
      s"""$block
         |# A row the outer join did not match leaves a hole, and pandas answers a
         |# hole in an integer column by reading the whole column as float. The
         |# engine leaves it INTEGER and writes a null, so read the declared ones
         |# back as the nullable integer that says the same thing.
         |_texera_int_names = $namesLit
         |for _texera_int_col in out1df.columns:
         |    # A renamed right column wears one "#@1" per collision it lost, so
         |    # take them off one at a time. A column that was declared under the
         |    # name as it stands is matched before any of them come off.
         |    _texera_base = _texera_int_col
         |    while _texera_base not in _texera_int_names and _texera_base.endswith("#@1"):
         |        _texera_base = _texera_base[:-3]
         |    if _texera_base in _texera_int_names:
         |        out1df[_texera_int_col] = out1df[_texera_int_col].astype("Int64")""".stripMargin
    }
  }

  override def generateStandaloneCode(): String = {
    val buildKeyLit = objectMapper.writeValueAsString(buildAttributeName)
    val probeKeyLit = objectMapper.writeValueAsString(probeAttributeName)
    val how = joinType match {
      case JoinType.INNER       => "inner"
      case JoinType.LEFT_OUTER  => "left"
      case JoinType.RIGHT_OUTER => "right"
      case JoinType.FULL_OUTER  => "outer"
    }
    // The engine renames a colliding right column by appending "#@1" until the
    // candidate is free, so a frame that already carries `x#@1` gets `x#@1#@1`.
    // pandas' `suffixes` appends once and then refuses the duplicate it just
    // made, so the rename is computed here the way HashJoinProbeOpExec computes
    // it, and the merge is handed columns that no longer collide.
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
         |out1df = in1df.merge(
         |    in2df.rename(columns=_rename),
         |    how=${pyStringLiteral(how)},
         |    left_on=$buildKeyLit,
         |    right_on=_probe_key,
         |)""".stripMargin
    // The probe key is not carried into the output: the build key already holds
    // it. `_probe_key` is whatever the rename above settled on, so this drops
    // the right column rather than a left one that happened to share its name.
    val tail =
      if (buildAttributeName != probeAttributeName)
        "out1df = out1df.drop(columns=[_probe_key]).reset_index(drop=True)"
      else
        "out1df = out1df.reset_index(drop=True)"
    s"$merge\n$tail"
  }
}
