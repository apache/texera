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

package org.apache.texera.amber.operator.statistics.columnsummary

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  InputPort,
  OutputPort,
  PhysicalOp,
  SchemaPropagationFunc
}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

class ColumnSummaryStatisticsOpDesc extends LogicalOp {

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Column Summary Statistics",
      "Computes per-column row count, null count, non-null count, min, max, and mean.",
      OperatorGroupConstants.AGGREGATE_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(blocking = true))
    )

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.statistics.columnsummary.ColumnSummaryStatisticsOpExec",
          objectMapper.writeValueAsString(ColumnSummaryStatisticsOpExecConfig())
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withParallelizable(false)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> outputSchema))
      )
  }

  private def outputSchema: Schema =
    Schema(
      List(
        new Attribute("columnName", AttributeType.STRING),
        new Attribute("dataType", AttributeType.STRING),
        new Attribute("rowCount", AttributeType.INTEGER),
        new Attribute("nullCount", AttributeType.INTEGER),
        new Attribute("nonNullCount", AttributeType.INTEGER),
        new Attribute("minValue", AttributeType.STRING),
        new Attribute("maxValue", AttributeType.STRING),
        new Attribute("meanValue", AttributeType.DOUBLE)
      )
    )
}