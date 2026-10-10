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

import com.twitter.util.Duration
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.pekko.util.Timeout
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.workflow.{
  ControlVariablePort,
  ExecutionMode,
  PortIdentity,
  WorkflowContext,
  WorkflowSettings
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.e2e.TestUtils.{
  buildWorkflow,
  cleanupWorkflowExecutionData,
  initiateTexeraDBForTestCases,
  runWorkflowAndReadResults,
  setUpWorkflowExecutionData,
  workflowContext
}
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.amber.operator.loop.{LoopEndOpDesc, LoopStartOpDesc}
import org.apache.texera.amber.operator.source.scan.FileAttributeType
import org.apache.texera.amber.operator.source.scan.file.FileScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.common.compiler.model.LogicalLink
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration.DurationInt

/**
  * The files example of the control-variable port, end to end: a loop reads every file of a list
  * with the system's unchanged file reader. LoopStart feeds the reader's control-variable port;
  * the reader's file name is "$file", which the loop sets in each iteration.
  *
  *   TextInput (one path per line) -> LoopStart -(control-variable port)-> File Scan("$file")
  *     -> LoopEnd
  *
  * The reader is a source: it has no data input, and the edge into its port is materialized, so
  * it starts once LoopStart's state has arrived, with "$file" bound.
  */
@IntegrationTest
class ControlVariablePortIntegrationSpec
    extends TestKit(ActorSystem("ControlVariablePortIntegrationSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  implicit val timeout: Timeout = Timeout(5.seconds)

  // 1-6 are taken by the other e2e/integration specs.
  private val specId = 7

  override protected def beforeEach(): Unit = setUpWorkflowExecutionData(specId)

  override protected def afterEach(): Unit = cleanupWorkflowExecutionData(specId)

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
    Class.forName("org.postgresql.Driver")
    initiateTexeraDBForTestCases()
  }

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private def materializedContext(): WorkflowContext =
    workflowContext(
      specId,
      WorkflowSettings(dataTransferBatchSize = 400, executionMode = ExecutionMode.MATERIALIZED)
    )

  "A loop" should "read every file of a list with an unchanged reader fed through its control-variable port" in {
    val dir = s"${TestOperators.parentDir}/src/test/resources/cvport"
    val paths = List("a.txt", "b.txt", "c.txt").map(name => s"$dir/$name")

    val list = new TextInputSourceOpDesc()
    list.textInput = paths.mkString("\n")
    val start = new LoopStartOpDesc()
    start.initialization = "i = 0\nfile = D.line[0]"
    start.output = "None"
    val reader = new FileScanSourceOpDesc()
    reader.fileName = Some("$file")
    reader.attributeType = FileAttributeType.INTEGER
    reader.attributeName = "n"
    val end = new LoopEndOpDesc()
    end.update = "i += 1\nfile = D.line.get(i)"
    end.condition = "i < len(D)"

    val values = runWorkflowAndReadResults(
      system,
      buildWorkflow(
        List(list, start, reader, end),
        List(
          LogicalLink(
            list.operatorIdentifier,
            PortIdentity(),
            start.operatorIdentifier,
            PortIdentity()
          ),
          LogicalLink(
            start.operatorIdentifier,
            PortIdentity(),
            reader.operatorIdentifier,
            ControlVariablePort.Id
          ),
          LogicalLink(
            reader.operatorIdentifier,
            PortIdentity(),
            end.operatorIdentifier,
            PortIdentity()
          )
        ),
        materializedContext()
      ),
      List(end.operatorIdentifier),
      _.get().map(_.getField[Any]("n").asInstanceOf[Number].intValue()).toList,
      Duration.fromSeconds(120)
    )(end.operatorIdentifier)

    // Three iterations, one per file, each file read exactly once.
    assert(values.sorted == (1 to 6).toList)
  }
}
