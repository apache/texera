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

package org.apache.texera.amber.operator.randomksampling

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.{StandaloneCodeGenerator, StandaloneHelpers}
import org.apache.texera.amber.operator.filter.FilterOpDesc
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

class RandomKSamplingOpDesc extends FilterOpDesc with StandaloneCodeGenerator {

  @JsonProperty(value = "random k sample percentage", required = true)
  @JsonPropertyDescription("random k sampling with given percentage")
  var percentage: Int = _

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
          "org.apache.texera.amber.operator.randomksampling.RandomKSamplingOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Random K Sampling",
      operatorDescription = "random sampling with given percentage",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

  override def standaloneHelpers(): Seq[String] = Seq(StandaloneHelpers.JavaRandom)

  // The executor is a per-row Bernoulli filter: keep iff (percentage/100.0) >=
  // rand.nextDouble(). Drawing from the same generator keeps the exact rows, not
  // merely the same distribution; each operator instance builds its own so
  // independent samplers don't share state.
  //
  // The seed is the worker count, so which rows survive is a property of the
  // deployment rather than of the workflow. A script is one process and states
  // the one seed it can, and the rows then agree with a single-worker run. They
  // will not agree with a wider one, and no seed would fix that: the tuples are
  // split across the workers and each samples its own share from the start of
  // the sequence, which one process reading the whole input cannot reproduce.
  // Sampling has no seed field for a user to pin, so nothing here promised a
  // particular set of rows in the first place.
  override def generateStandaloneCode(): String = {
    val p = percentage / 100.0
    s"""_texera_rks_rng = _TexeraJavaRandom(1)
       |_texera_rks_mask = pd.Series(
       |    [$p >= _texera_rks_rng.next_double() for _ in range(len(in1df))],
       |    index=in1df.index, dtype=bool
       |)
       |out1df = in1df[_texera_rks_mask].reset_index(drop=True)""".stripMargin
  }
}
