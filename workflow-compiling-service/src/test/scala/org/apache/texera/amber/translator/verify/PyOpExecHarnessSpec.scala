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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, LargeBinary, Schema, Tuple}
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

  /** Reads a LARGE_BINARY column and writes out the reference it holds.
    *
    * The bytes of a large binary live in S3 and the field is the s3:// URI that
    * points at them, so an operator can be handed one, and can hand one back,
    * without any storage being reachable.
    */
  private class LargeBinaryOpDesc(readsInput: Boolean) extends PythonOperatorDescriptor {
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        userFriendlyName = "Large Binary",
        operatorDescription = "reads or writes a reference to a large binary",
        operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
        inputPorts = List(InputPort()),
        outputPorts = List(OutputPort())
      )

    override def getOutputSchemas(
        inputSchemas: Map[PortIdentity, Schema]
    ): Map[PortIdentity, Schema] =
      Map(
        operatorInfo.outputPorts.head.id ->
          (if (readsInput) Schema().add(new Attribute("uri", AttributeType.STRING))
           else Schema().add(new Attribute("blob", AttributeType.LARGE_BINARY)))
      )

    override def generatePythonCode(): String =
      if (readsInput)
        """from pytexera import *
          |
          |class ProcessTupleOperator(UDFOperatorV2):
          |
          |    @overrides
          |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
          |        yield {"uri": tuple_["blob"].uri}
          |""".stripMargin
      else
        """from pytexera import *
          |
          |class ProcessTupleOperator(UDFOperatorV2):
          |
          |    @overrides
          |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
          |        yield {"blob": largebinary(tuple_["uri"])}
          |""".stripMargin
  }

  /** Reports the Python type of the BINARY cell it is handed. */
  private class BinaryTypeOpDesc extends PythonOperatorDescriptor {
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        userFriendlyName = "Binary Type",
        operatorDescription = "reports the type of a binary cell",
        operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
        inputPorts = List(InputPort()),
        outputPorts = List(OutputPort())
      )

    override def getOutputSchemas(
        inputSchemas: Map[PortIdentity, Schema]
    ): Map[PortIdentity, Schema] =
      Map(
        operatorInfo.outputPorts.head.id -> Schema().add(
          new Attribute("kind", AttributeType.STRING)
        )
      )

    override def generatePythonCode(): String =
      """from pytexera import *
        |
        |class ProcessTupleOperator(UDFOperatorV2):
        |
        |    @overrides
        |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:
        |        yield {"kind": type(tuple_["blob"]).__name__}
        |""".stripMargin
  }

  private def binaryCellKind(cell: Array[Byte]): String = {
    val dir = Files.createTempDirectory("py-op-harness-binary-")
    val inputSchema = Schema().add(new Attribute("blob", AttributeType.BINARY))
    val input = dir.resolve("input_port_0.jsonl")
    val row = Tuple.builder(inputSchema).add("blob", AttributeType.BINARY, cell).build()
    TupleIO.writeTuples(input, Iterator(row), inputSchema)

    val result = PyOpExecHarness.execute(
      new BinaryTypeOpDesc,
      inputs = Map(PortIdentity(0) -> input),
      outputDir = dir.resolve("actual")
    )

    val out = result.outputs(PortIdentity(0))
    TupleIO
      .readTuples(out, TupleIO.readSchemaSidecar(out))
      .map(_.getField[String]("kind"))
      .toSeq
      .head
  }

  // A model column arrives as the marker followed by the pickle, and the worker
  // unpickles it before the operator sees it. This is pickle.dumps(["a"],
  // protocol=0), which stands in for a fitted estimator.
  "PyOpExecHarness" should "hand a pickled binary cell to the operator as the object" in {
    val pickled = "pickle    ".getBytes("US-ASCII") ++ "(lp0\nVa\np1\na.".getBytes("US-ASCII")
    binaryCellKind(pickled) shouldBe "list"
  }

  it should "hand any other binary cell to the operator as bytes" in {
    binaryCellKind(Array[Byte](0, 1, 2)) shouldBe "bytes"
  }

  private val someObject = "s3://a-bucket/a/large/object"

  // The schema sidecar names large_binary, so the harness has to carry it in as
  // well as out. The operator reads the reference itself, which a column handed
  // over as plain text could not answer for.
  "PyOpExecHarness" should "hand a large binary column to the operator as a reference" in {
    val dir = Files.createTempDirectory("py-op-harness-large-binary-in-")
    val inputSchema = Schema().add(new Attribute("blob", AttributeType.LARGE_BINARY))
    val input = dir.resolve("input_port_0.jsonl")
    val row = Tuple
      .builder(inputSchema)
      .add("blob", AttributeType.LARGE_BINARY, new LargeBinary(someObject))
      .build()
    TupleIO.writeTuples(input, Iterator(row), inputSchema)

    val result = PyOpExecHarness.execute(
      new LargeBinaryOpDesc(readsInput = true),
      inputs = Map(PortIdentity(0) -> input),
      outputDir = dir.resolve("actual")
    )

    val out = result.outputs(PortIdentity(0))
    TupleIO
      .readTuples(out, TupleIO.readSchemaSidecar(out))
      .map(_.getField[String]("uri"))
      .toSeq shouldBe Seq(someObject)
  }

  it should "write back a large binary the operator made" in {
    val dir = Files.createTempDirectory("py-op-harness-large-binary-out-")
    val inputSchema = Schema().add(new Attribute("uri", AttributeType.STRING))
    val input = dir.resolve("input_port_0.jsonl")
    val row = Tuple.builder(inputSchema).add("uri", AttributeType.STRING, someObject).build()
    TupleIO.writeTuples(input, Iterator(row), inputSchema)

    val result = PyOpExecHarness.execute(
      new LargeBinaryOpDesc(readsInput = false),
      inputs = Map(PortIdentity(0) -> input),
      outputDir = dir.resolve("actual")
    )

    val out = result.outputs(PortIdentity(0))
    // Read back as a LargeBinary and not as the text of one: the column is
    // declared large_binary, and this is the type the rest of the pipeline gets.
    TupleIO
      .readTuples(out, TupleIO.readSchemaSidecar(out))
      .map(_.getField[LargeBinary]("blob").getUri)
      .toSeq shouldBe Seq(someObject)
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
