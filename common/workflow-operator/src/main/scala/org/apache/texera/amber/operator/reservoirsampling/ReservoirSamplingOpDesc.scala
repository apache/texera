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

package org.apache.texera.amber.operator.reservoirsampling

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator, StandaloneHelpers}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

class ReservoirSamplingOpDesc extends LogicalOp with StandaloneCodeGenerator {

  @JsonProperty(value = "number of item sampled in reservoir sampling", required = true)
  @JsonPropertyDescription("reservoir sampling with k items being kept randomly")
  var k: Int = _

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
          "org.apache.texera.amber.operator.reservoirsampling.ReservoirSamplingOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      userFriendlyName = "Reservoir Sampling",
      operatorDescription = "Reservoir Sampling with k items being kept randomly",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
  }

  override def standaloneHelpers(): Seq[String] = Seq(StandaloneHelpers.JavaRandom)

  // The executor runs Algorithm R (Vitter): fill the reservoir with the first k
  // tuples, then for tuple m+1 (m >= k) draw i = rand.nextInt(m), uniform in
  // [0, m), and replace reservoir[i] iff i < k. Its generator is seeded with the
  // worker count — 1 in the single-worker setup a translated script reproduces.
  override def generateStandaloneCode(): String = {
    s"""_texera_rs_rng = _TexeraJavaRandom(1)
       |_texera_rs_k = $k
       |_texera_rs_reservoir = []
       |for _texera_rs_n, _texera_rs_row in enumerate(in1df.itertuples(index=False, name=None)):
       |    if _texera_rs_n < _texera_rs_k:
       |        _texera_rs_reservoir.append(_texera_rs_row)
       |    else:
       |        _texera_rs_i = _texera_rs_rng.next_int(_texera_rs_n)
       |        if _texera_rs_i < _texera_rs_k:
       |            _texera_rs_reservoir[_texera_rs_i] = _texera_rs_row
       |out1df = pd.DataFrame(_texera_rs_reservoir, columns=list(in1df.columns)).reset_index(drop=True)""".stripMargin
  }
}
