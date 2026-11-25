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

package org.apache.texera.amber.engine.e2e

import com.typesafe.scalalogging.Logger
import org.apache.texera.amber.core.executor.{OpExecInitInfo, OpExecWithCode}
import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.{LogicalOp, TestOperators}
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.common.compiler.model.LogicalLink

/**
  * E2E reconfiguration tests that spawn Python UDF workers. Routed to the
  * `amber-integration` CI job via the class-level `@IntegrationTest` tag,
  * which provisions Python deps; the lighter `amber` job excludes this tag.
  *
  * Pure-Scala reconfiguration tests live in [[ReconfigurationSpec]] and run
  * in the regular `amber` job.
  */
@IntegrationTest
class ReconfigurationIntegrationSpec
    extends RetryingDatabaseE2ESpec("ReconfigurationIntegrationSpec", specId = 4) {

  val logger = Logger("ReconfigurationIntegrationSpecLogger")
  val ctx = TestUtils.workflowContext(specId)

  override def beforeAll(): Unit = {
    super.beforeAll()
    warmUpPythonWorker(logger)
  }

  // Thin wrapper around the shared TestUtils helper so call sites below stay
  // ctx/system-implicit. The actual workflow-driver logic lives in TestUtils
  // and is reused by ReconfigurationSpec.
  def shouldReconfigure(
      operators: List[LogicalOp],
      links: List[LogicalLink],
      targetOps: Seq[LogicalOp],
      newOpExecInitInfo: OpExecInitInfo
  ): Map[OperatorIdentity, List[Tuple]] =
    TestUtils.shouldReconfigure(system, ctx, operators, links, targetOps, newOpExecInitInfo)

  // Small source that emits slowly (30 rows, 0.25s apart) so a pause lands
  // mid-run and the workflow still completes quickly after resume.
  private def slowSource() =
    TestOperators.slowRegionSourceOpDesc(numTuple = 30, delaySeconds = 0.25)

  private val reconfiguredUdfCode =
    """
      |from pytexera import *
      |
      |class ProcessTupleOperator(UDFOperatorV2):
      |    @overrides
      |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
      |        tuple_['Region'] = tuple_['Region'] + '_reconfigured'
      |        yield tuple_
      |""".stripMargin

  "Engine" should "be able to modify a python UDF worker in workflow" in {
    val sourceOpDesc = slowSource()
    val udfOpDesc = TestOperators.pythonOpDesc()

    val result = shouldReconfigure(
      List(sourceOpDesc, udfOpDesc),
      List(
        LogicalLink(
          sourceOpDesc.operatorIdentifier,
          PortIdentity(),
          udfOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      Seq(udfOpDesc),
      OpExecWithCode(reconfiguredUdfCode, "python")
    )
    assert(result(udfOpDesc.operatorIdentifier).exists { t =>
      t.getField("Region").asInstanceOf[String].contains("_reconfigured")
    })
  }

  "Engine" should "propagate reconfiguration through a source operator in workflow" in {
    val sourceOpDesc = slowSource()
    val udfOpDesc = TestOperators.pythonOpDesc()
    val result = shouldReconfigure(
      List(sourceOpDesc, udfOpDesc),
      List(
        LogicalLink(
          sourceOpDesc.operatorIdentifier,
          PortIdentity(),
          udfOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      Seq(udfOpDesc),
      OpExecWithCode(reconfiguredUdfCode, "python")
    )
    assert(result(udfOpDesc.operatorIdentifier).exists { t =>
      t.getField("Region").asInstanceOf[String].contains("_reconfigured")
    })
  }

  "Engine" should "be able to modify two python UDFs in workflow" in {
    val sourceOpDesc = slowSource()
    val udfOpDesc1 = TestOperators.pythonOpDesc()
    val udfOpDesc2 = TestOperators.pythonOpDesc()

    val result = shouldReconfigure(
      List(sourceOpDesc, udfOpDesc1, udfOpDesc2),
      List(
        LogicalLink(
          sourceOpDesc.operatorIdentifier,
          PortIdentity(),
          udfOpDesc1.operatorIdentifier,
          PortIdentity()
        ),
        LogicalLink(
          udfOpDesc1.operatorIdentifier,
          PortIdentity(),
          udfOpDesc2.operatorIdentifier,
          PortIdentity()
        )
      ),
      Seq(udfOpDesc1, udfOpDesc2),
      OpExecWithCode(reconfiguredUdfCode, "python")
    )
    assert(result(udfOpDesc2.operatorIdentifier).exists { t =>
      t.getField("Region").asInstanceOf[String].contains("_reconfigured_reconfigured")
    })
  }

}
