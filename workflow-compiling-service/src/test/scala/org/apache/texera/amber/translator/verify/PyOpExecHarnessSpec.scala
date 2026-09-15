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

package org.apache.texera.amber.translator.verify

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.translator.verify.tags.IntegrationTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

/** The reference path's own behaviour, where it can differ from the worker's.
  *
  * Tagged @IntegrationTest: the harness forks Python.
  */
@IntegrationTest
class PyOpExecHarnessSpec extends AnyFlatSpec with Matchers {

  /** Yields one dict twice, changing it in between. `DataProcessor` reads a
    * yield where it happens, so the engine records 1 and then 2; a driver that
    * collects the yields first and converts them afterwards sees the same
    * object twice and records 2 twice.
    */
  private class MutatingYieldOpDesc extends PythonOperatorDescriptor {
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        userFriendlyName = "Mutating Yield",
        operatorDescription = "yields one dict twice, changing it in between",
        operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
        inputPorts = List(InputPort()),
        outputPorts = List(OutputPort())
      )

    override def getOutputSchemas(
        inputSchemas: Map[PortIdentity, Schema]
    ): Map[PortIdentity, Schema] =
      Map(
        operatorInfo.outputPorts.head.id -> Schema().add(new Attribute("x", AttributeType.INTEGER))
      )

    override def generatePythonCode(): String =
      """from pytexera import *
        |
        |class ProcessTableOperator(UDFTableOperator):
        |
        |    @overrides
        |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        |        row = {"x": 1}
        |        yield row
        |        row["x"] = 2
        |        yield row
        |""".stripMargin
  }

  "PyOpExecHarness" should "record each yield as it was yielded" in {
    val dir = Files.createTempDirectory("py-op-harness-")
    val inputSchema = Schema().add(new Attribute("seed", AttributeType.INTEGER))
    val input = dir.resolve("input_port_0.jsonl")
    val seed = Tuple.builder(inputSchema).add("seed", AttributeType.INTEGER, Int.box(1)).build()
    TupleIO.writeTuples(input, Iterator(seed), inputSchema)

    val result = PyOpExecHarness.execute(
      new MutatingYieldOpDesc,
      inputs = Map(PortIdentity(0) -> input),
      outputDir = dir.resolve("actual")
    )

    val out = result.outputs(PortIdentity(0))
    val values = TupleIO
      .readTuples(out, TupleIO.readSchemaSidecar(out))
      .map(_.getField[Integer]("x"))
      .toSeq
    withClue(s"schema was $inputSchema\n") {
      values shouldBe Seq(1, 2)
    }
  }
}
