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

package org.apache.texera.amber.operator.sortPartitions

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.state.NoLoopVariable
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp, RangePartition}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "sortAttributeName":{
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class SortPartitionsOpDesc extends LogicalOp with StandaloneCodeGenerator {

  // Sorting is this operator's contract: output row order is meaningful.
  override def orderSensitive: Boolean = true

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute")
  @JsonPropertyDescription("Attribute to sort (must be numerical).")
  @AutofillAttributeName
  @NoLoopVariable
  var sortAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Domain Min")
  @JsonPropertyDescription("Minimum value of the domain of the attribute.")
  @NoLoopVariable
  var domainMin: Long = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Domain Max")
  @JsonPropertyDescription("Maximum value of the domain of the attribute.")
  @NoLoopVariable
  var domainMax: Long = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.sortPartitions.SortPartitionsOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(
        List(Option(RangePartition(List(sortAttributeName), domainMin, domainMax)))
      )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sort Partitions",
      "Sort Partitions",
      OperatorGroupConstants.SORT_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(blocking = true))
    )

  // Row order is this operator's answer, so the sort matches the engine's:
  // stable, ascending, nulls first. The domain bounds are partitioning hints
  // that the sort itself never reads.
  //
  // The engine orders a NaN after positive infinity. In a nullable column,
  // which is what an Arrow file is read into, pandas keeps a NaN apart from a
  // null and sorts it last too. A numpy float column has one slot for both, so
  // there a NaN goes first with the nulls.
  //
  // The rest of AttributeTypeUtils.compare is written out as tiers of a key
  // frame, as in StableMergeSortOpDesc. Double.compare puts -0.0 below 0.0,
  // where pandas reads the two as equal. A timestamp is compared by getTime,
  // so two inside one millisecond keep their input order. A string compares
  // UTF-16 code units, where pandas compares code points, and the two disagree
  // once a character above U+FFFF meets one between U+E000 and U+FFFF.
  override def generateStandaloneCode(): String = {
    val col = pyStringLiteral(Option(sortAttributeName).getOrElse(""))
    s"""import math
       |out1df = in1df.reset_index(drop=True)
       |_texera_v = out1df[$col]
       |_texera_keys = pd.DataFrame(index=out1df.index)
       |_texera_keys["held"] = _texera_v.notna()
       |_texera_keys["nan"] = (_texera_v != _texera_v).fillna(False)
       |if pd.api.types.is_datetime64_any_dtype(_texera_v.dtype):
       |    _texera_keys["val"] = _texera_v.dt.floor("ms")
       |elif _texera_v.dtype == object or isinstance(_texera_v.dtype, pd.StringDtype):
       |    _texera_keys["val"] = _texera_v.map(
       |        lambda v: v.encode("utf-16-be") if isinstance(v, str) else v
       |    )
       |else:
       |    _texera_keys["val"] = _texera_v
       |_texera_by = ["held", "nan", "val"]
       |if pd.api.types.is_float_dtype(_texera_v.dtype):
       |    _texera_keys["sign"] = [
       |        1 if missing or v != v or math.copysign(1.0, v) > 0 else 0
       |        for v, missing in zip(_texera_v, _texera_v.isna())
       |    ]
       |    _texera_by.append("sign")
       |out1df = out1df.loc[
       |    _texera_keys.sort_values(by=_texera_by, kind="mergesort").index
       |].reset_index(drop=True)""".stripMargin
  }

}
