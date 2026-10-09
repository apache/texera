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

import com.google.protobuf.timestamp.Timestamp
import com.twitter.util.Duration
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.WorkflowRuntimeException
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.coordinator.Workflow
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  ConsoleMessage,
  ConsoleMessageType
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.e2e.TestUtils.{
  buildWorkflow,
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  runWorkflowAndReadTerminalResults,
  setUpWorkflowExecutionData,
  workerError
}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.typecasting.{TypeCastingOpDesc, TypeCastingUnit}
import org.apache.texera.common.compiler.model.LogicalLink
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class TestUtilsSpec
    extends TestKit(ActorSystem("TestUtilsSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  // Unique per-suite id, so the seeded rows and result tables don't collide
  // with the other e2e suites.
  private val specId = 8

  // Far longer than the workflow takes, so reaching it means the run was
  // waited out rather than ended.
  private val completionTimeout = Duration.fromSeconds(60)

  override protected def beforeEach(): Unit = setUpWorkflowExecutionData(specId)

  override protected def afterEach(): Unit = cleanupWorkflowExecutionData(specId)

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  /** csv (100 rows) -> TypeCasting of `attribute` to INTEGER. */
  private def castToIntegerWorkflow(attribute: String): (Workflow, TypeCastingOpDesc) = {
    val csvOpDesc = TestOperators.smallCsvScanOpDesc()
    val castingUnit = new TypeCastingUnit()
    castingUnit.attribute = attribute
    castingUnit.resultType = AttributeType.INTEGER
    val castOpDesc = new TypeCastingOpDesc()
    castOpDesc.typeCastingUnits = List(castingUnit)
    val workflow = buildWorkflow(
      List(csvOpDesc, castOpDesc),
      List(
        LogicalLink(
          csvOpDesc.operatorIdentifier,
          PortIdentity(),
          castOpDesc.operatorIdentifier,
          PortIdentity()
        )
      ),
      TestUtils.workflowContext(specId)
    )
    (workflow, castOpDesc)
  }

  "runWorkflowAndReadTerminalResults" should "fail with the error of an operator that throws on a tuple" in {
    // "Region" holds text such as "Asia", so the cast throws on the first tuple.
    // The worker reports the error and pauses itself instead of failing the
    // run, so the run never reaches COMPLETED.
    val (workflow, castOpDesc) = castToIntegerWorkflow("Region")
    val error = intercept[WorkflowRuntimeException] {
      runWorkflowAndReadTerminalResults(system, workflow, completionTimeout)
    }
    assert(error.getMessage.contains("Failed to parse type java.lang.String to Integer"))
    // The failing worker is carried, as it is for an error raised as a FatalError.
    assert(error.relatedWorkerId.exists(_.name.contains(castOpDesc.operatorIdentifier.id)))
  }

  it should "return the results when no operator throws" in {
    // "Units Sold" holds whole numbers, so the same workflow completes.
    val (workflow, castOpDesc) = castToIntegerWorkflow("Units Sold")
    val results = runWorkflowAndReadTerminalResults(system, workflow, completionTimeout)
    val castTuples = results(castOpDesc.operatorIdentifier)
    assert(castTuples.size == 100)
    assert(
      castTuples.forall(_.getSchema.getAttribute("Units Sold").getType == AttributeType.INTEGER)
    )
  }

  private val workerId = "Worker:WF8-SomeOpDesc-main-0"

  private def consoleMessage(
      msgType: ConsoleMessageType,
      message: String = "at Foo.bar(Foo.scala:1)"
  ): ConsoleMessage =
    ConsoleMessage(
      workerId,
      Timestamp(),
      msgType,
      "(Foo.scala:1)",
      "java.lang.Exception: boom",
      message
    )

  "workerError" should "turn an ERROR console message into the worker's error" in {
    val error = workerError(consoleMessage(ConsoleMessageType.ERROR)).get
    assert(error.getMessage == "java.lang.Exception: boom\nat Foo.bar(Foo.scala:1)")
    assert(error.relatedWorkerId.contains(ActorVirtualIdentity(workerId)))
  }

  it should "leave out an empty stack trace" in {
    val error = workerError(consoleMessage(ConsoleMessageType.ERROR, message = "")).get
    assert(error.getMessage == "java.lang.Exception: boom")
  }

  it should "ignore console messages that are not errors" in {
    Seq(
      ConsoleMessageType.PRINT,
      ConsoleMessageType.COMMAND,
      ConsoleMessageType.DEBUGGER,
      ConsoleMessageType.Unrecognized(99)
    ).foreach { msgType =>
      assert(workerError(consoleMessage(msgType)).isEmpty, msgType)
    }
  }
}
