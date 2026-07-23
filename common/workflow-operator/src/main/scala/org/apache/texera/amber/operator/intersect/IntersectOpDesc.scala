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

package org.apache.texera.amber.operator.intersect

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}

class IntersectOpDesc extends LogicalOp with StandaloneCodeGenerator {

  // set/bag semantics: output row order is implementation-defined
  override def outputOrderSignificant: Boolean = false

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName("org.apache.texera.amber.operator.intersect.IntersectOpExec")
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(List(Option(HashPartition()), Option(HashPartition())))
      .withDerivePartition(_ => HashPartition())
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Intersect",
      "Take the intersect of two inputs",
      OperatorGroupConstants.SET_GROUP,
      inputPorts = List(InputPort(PortIdentity()), InputPort(PortIdentity(1))),
      outputPorts = List(OutputPort(blocking = true))
    )

  // Distinct rows in both inputs. concat + duplicated (not pd.merge) so NaN ==
  // NaN matches the JVM HashSet's null equality.
  override def generateStandaloneCode(): String =
    """_both = pd.concat([in1df.drop_duplicates(), in2df.drop_duplicates()], ignore_index=True)
      |out1df = _both[_both.duplicated(keep="first")].reset_index(drop=True)""".stripMargin
}
