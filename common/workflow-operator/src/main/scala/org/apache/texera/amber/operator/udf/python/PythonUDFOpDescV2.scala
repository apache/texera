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

package org.apache.texera.amber.operator.udf.python

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.{Attribute, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.{LogicalOp, PortDescription, StateTransferFunc}

import scala.util.{Success, Try}

class PythonUDFOpDescV2 extends LogicalOp {
  @JsonProperty(
    required = true,
    defaultValue =
      "# Choose from the following templates:\n" +
        "# \n" +
        "# from pytexera import *\n" +
        "# \n" +
        "# class ProcessTupleOperator(UDFOperatorV2):\n" +
        "#     \n" +
        "#     @overrides\n" +
        "#     def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:\n" +
        "#         yield tuple_\n" +
        "# \n" +
        "# class ProcessBatchOperator(UDFBatchOperator):\n" +
        "#     BATCH_SIZE = 10 # must be a positive integer\n" +
        "# \n" +
        "#     @overrides\n" +
        "#     def process_batch(self, batch: Batch, port: int) -> Iterator[Optional[BatchLike]]:\n" +
        "#         yield batch\n" +
        "# \n" +
        "# class ProcessTableOperator(UDFTableOperator):\n" +
        "# \n" +
        "#     @overrides\n" +
        "#     def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:\n" +
        "#         yield table\n"
  )
  @JsonSchemaTitle("Python script")
  @JsonPropertyDescription("Input your code here")
  var code: String = ""

  @JsonProperty(required = true, defaultValue = "1")
  @JsonSchemaTitle("Worker count")
  @JsonPropertyDescription("Specify how many parallel workers to launch")
  var workers: Int = Int.box(1)

  @JsonProperty(required = true, defaultValue = "true")
  @JsonSchemaTitle("Retain input columns")
  @JsonPropertyDescription("Keep the original input columns?")
  var retainInputColumns: Boolean = Boolean.box(false)

  @JsonProperty
  @JsonSchemaTitle("Extra output column(s)")
  @JsonPropertyDescription(
    "Name of the newly added output columns that the UDF will produce, if any"
  )
  var outputColumns: List[Attribute] = List()

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    Preconditions.checkArgument(workers >= 1, "Need at least 1 worker.", Array())
    val opInfo = this.operatorInfo
    val partitionRequirement: List[Option[PartitionInfo]] = if (inputPorts != null) {
      inputPorts.map(p => Option(p.partitionRequirement))
    } else {
      opInfo.inputPorts.map(_ => None)
    }

    val propagateSchema = (inputSchemas: Map[PortIdentity, Schema]) => {
      val inputSchema = inputSchemas(operatorInfo.inputPorts.head.id)
      var outputSchema = if (retainInputColumns) inputSchema else Schema()

      // Add custom output columns if defined
      if (outputColumns != null) {
        if (retainInputColumns) {
          // Check for duplicate column names
          for (column <- outputColumns) {
            if (inputSchema.containsAttribute(column.getName)) {
              throw new RuntimeException(s"Column name ${column.getName} already exists!")
            }
          }
        }
        // Add output columns to the schema
        outputSchema = outputSchema.add(outputColumns)
      }

      Map(operatorInfo.outputPorts.head.id -> outputSchema)
    }

    val physicalOp = if (workers > 1) {
      PhysicalOp
        .oneToOnePhysicalOp(
          workflowId,
          executionId,
          operatorIdentifier,
          OpExecWithCode(code, "python")
        )
        .withParallelizable(true)
        .withSuggestedWorkerNum(workers)
    } else {
      PhysicalOp
        .manyToOnePhysicalOp(
          workflowId,
          executionId,
          operatorIdentifier,
          OpExecWithCode(code, "python")
        )
        .withParallelizable(false)
    }

    physicalOp
      .withDerivePartition(_ => UnknownPartition())
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(partitionRequirement)
      .withIsOneToManyOp(true)
      .withPropagateSchema(SchemaPropagationFunc(propagateSchema))
  }

  override def operatorInfo: OperatorInfo = {
    val inputPortInfo = if (inputPorts != null) {
      inputPorts.zipWithIndex.map {
        case (portDesc: PortDescription, idx) =>
          InputPort(
            PortIdentity(idx),
            displayName = portDesc.displayName,
            disallowMultiLinks = portDesc.disallowMultiInputs,
            dependencies = portDesc.dependencies.map(idx => PortIdentity(idx))
          )
      }
    } else {
      List(InputPort())
    }
    val outputPortInfo = if (outputPorts != null) {
      outputPorts.zipWithIndex.map {
        case (portDesc, idx) => OutputPort(PortIdentity(idx), displayName = portDesc.displayName)
      }
    } else {
      List(OutputPort())
    }

    OperatorInfo(
      "Python UDF",
      """User-defined function operator in Python script.
        |There are 2 APIs to process data:
        |
        |## Tuple API
        |Takes one input tuple from a port at a time. Returns an iterator of optional TupleLike instances.
        |Use cases: Functional operations applied to tuples one by one (map, reduce, filter).
        |
        |Template:
        |```python
        |from pytexera import *
        |
        |class ProcessTupleOperator(UDFOperatorV2):
        |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        |        yield tuple_
        |```
        |
        |Example - Filter tuples by conditions:
        |```python
        |from pytexera import *
        |
        |class ProcessTupleOperator(UDFOperatorV2):
        |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        |        q = tuple_["QUANTITY"]
        |        oq = tuple_["ORDERED_QUANTITY"]
        |        p = tuple_["UNIT_PRICE"]
        |        if q is not None and oq is not None and p is not None:
        |            if q <= oq and p >= 0:
        |                yield tuple_
        |```
        |
        |## Table API
        |Consumes a whole Table (pandas DataFrame) from a port. Returns an iterator of optional TableLike instances.
        |Use cases: Blocking operations that consume the whole table.
        |
        |Template:
        |```python
        |from pytexera import *
        |
        |class ProcessTableOperator(UDFTableOperator):
        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        |        yield table
        |```
        |
        |Example - Filter DataFrame rows:
        |```python
        |from pytexera import *
        |import pandas as pd
        |
        |class ProcessTableOperator(UDFTableOperator):
        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        |        df: pd.DataFrame = table
        |        m1 = (df["KWMENG"].notna()) & (df["KBMENG"].notna()) & (df["KWMENG"] <= df["KBMENG"])
        |        m2 = (df["NET_VALUE"].notna()) & (df["NET_VALUE"] >= 0)
        |        yield df[m1 & m2]
        |```
        |
        |## Important Rules
        |
        |- DO NOT change the class name (ProcessTupleOperator or ProcessTableOperator).
        |- Import packages explicitly (pandas, numpy, etc.).
        |- Tuple is a Python dict. Access fields with tuple_["field"] ONLY (no .get/.set/.values).
        |- Table is a pandas DataFrame.
        |- Use yield to return results.
        |- Handle None values carefully.
        |- Do not cast types.
        |- Keep each UDF focused on one task.
        |- Only change the python code property, not other properties.
        |- If adding extra columns, specify them in the Extra Output Columns property.
        |- Prefer native operators over Python UDF when possible.
        |""".stripMargin,
      OperatorGroupConstants.PYTHON_GROUP,
      inputPortInfo,
      outputPortInfo,
      dynamicInputPorts = true,
      dynamicOutputPorts = true,
      supportReconfiguration = true,
      allowPortCustomization = true
    )
  }
  override def runtimeReconfiguration(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      oldLogicalOp: LogicalOp,
      newLogicalOp: LogicalOp
  ): Try[(PhysicalOp, Option[StateTransferFunc])] = {
    Success(newLogicalOp.getPhysicalOp(workflowId, executionId), None)
  }
}
