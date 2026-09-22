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

package org.apache.texera.amber.operator.sort

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.mutable.ListBuffer

/**
  * This operator performs a stable, per-partition sort using an incremental
  * stack of sorted buckets and pairwise stable merges. The sort keys define
  * the lexicographic order and per-key direction (ASC/DESC).
  */
//TODO(#3922): disallowing sorting on binary type
class StableMergeSortOpDesc extends LogicalOp with StandaloneCodeGenerator {

  // Sorting is this operator's contract: output row order is meaningful.
  override def orderSensitive: Boolean = true

  @JsonProperty(value = "keys", required = true)
  @JsonSchemaTitle("Sort Keys")
  @JsonPropertyDescription("List of attributes to sort by with ordering preferences")
  var keys: ListBuffer[SortCriteriaUnit] = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .manyToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.sort.StableMergeSortOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Stable Merge Sort",
      "Stable per-partition sort with multi-key ordering (incremental stack of sorted buckets)",
      OperatorGroupConstants.SORT_GROUP,
      List(InputPort()),
      List(OutputPort(blocking = true))
    )

  // The engine runs an incremental stable merge sort with nulls last whichever
  // way a key points. pandas' mergesort is stable too, so the ordering below is
  // the same one.
  //
  // A string column parts more narrowly: the engine reads UTF-16 code units and
  // pandas reads code points, which agree below U+FFFF and can differ above it.
  override def generateStandaloneCode(): String = {
    val criteria = Option(keys).getOrElse(ListBuffer.empty)
    if (criteria.isEmpty) return "out1df = in1df.copy()"
    val cols = criteria
      .map(c => pyStringLiteral(c.attributeName))
      .mkString("[", ", ", "]")
    val ascending = criteria
      .map(c => if (c.sortPreference == SortPreference.ASC) "True" else "False")
      .mkString("[", ", ", "]")
    // Sort each key in three tiers, because the engine treats a null and a NaN
    // differently: a null goes last whichever way the key points, while a NaN
    // compares above every number, so it goes last ascending and first
    // descending. A column read into a numpy dtype has one slot for both, and
    // there both land in the null tier, which is where they were before.
    //
    // The tiers are built in a frame of their own and named by position, so
    // the input keeps every column it arrived with: a helper named after the
    // key would overwrite an input column that already answers to that name,
    // and dropping the helper afterwards would take the payload with it.
    s"""_texera_sorted = in1df.reset_index(drop=True)
       |_texera_keys = pd.DataFrame(index=_texera_sorted.index)
       |_texera_by = []
       |_texera_asc = []
       |for _texera_i, (_texera_col, _texera_a) in enumerate(zip($cols, $ascending)):
       |    _texera_null = "null_" + str(_texera_i)
       |    _texera_nan = "nan_" + str(_texera_i)
       |    _texera_val = "val_" + str(_texera_i)
       |    _texera_keys[_texera_null] = _texera_sorted[_texera_col].isna()
       |    _texera_keys[_texera_nan] = (
       |        _texera_sorted[_texera_col] != _texera_sorted[_texera_col]
       |    ).fillna(False)
       |    _texera_keys[_texera_val] = _texera_sorted[_texera_col]
       |    _texera_by += [_texera_null, _texera_nan, _texera_val]
       |    _texera_asc += [True, _texera_a, _texera_a]
       |out1df = _texera_sorted.loc[
       |    _texera_keys.sort_values(
       |        by=_texera_by, ascending=_texera_asc, kind="mergesort"
       |    ).index
       |].reset_index(drop=True)""".stripMargin
  }
}
