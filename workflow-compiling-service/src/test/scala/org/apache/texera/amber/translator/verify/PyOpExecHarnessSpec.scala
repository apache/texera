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

import java.nio.file.{Files, Path}
import java.sql.Timestamp
import java.util.Base64
import scala.sys.process._

/** The reference path's own behaviour, where it can differ from the worker's.
  *
  * Tagged @IntegrationTest: the harness forks Python.
  */
@IntegrationTest
class PyOpExecHarnessSpec extends AnyFlatSpec with Matchers {

  /** A Python operator with one input and one output port that declares one
    * output column and runs the code it is given.
    */
  private class InlinePythonOpDesc(
      name: String,
      desc: String,
      outName: String,
      outType: AttributeType,
      pythonCode: String
  ) extends PythonOperatorDescriptor {
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        userFriendlyName = name,
        operatorDescription = desc,
        operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
        inputPorts = List(InputPort()),
        outputPorts = List(OutputPort())
      )

    override def getOutputSchemas(
        inputSchemas: Map[PortIdentity, Schema]
    ): Map[PortIdentity, Schema] =
      Map(operatorInfo.outputPorts.head.id -> Schema().add(new Attribute(outName, outType)))

    override def generatePythonCode(): String = pythonCode
  }

  /** Puts `bodyLines` inside `process_tuple`, or inside `process_table` when
    * `tableOp` is set.
    */
  private def inlinePyOp(
      name: String,
      desc: String,
      outName: String,
      outType: AttributeType,
      bodyLines: Seq[String],
      tableOp: Boolean = false
  ): PythonOperatorDescriptor = {
    val header =
      if (tableOp)
        """class ProcessTableOperator(UDFTableOperator):
          |
          |    @overrides
          |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:""".stripMargin
      else
        """class ProcessTupleOperator(UDFOperatorV2):
          |
          |    @overrides
          |    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:""".stripMargin
    val body = bodyLines.map("        " + _).mkString("\n")
    val code =
      s"""from pytexera import *
         |
         |$header
         |$body
         |""".stripMargin
    new InlinePythonOpDesc(name, desc, outName, outType, code)
  }

  /** Yields one dict twice, changing it in between. `DataProcessor` reads a
    * yield where it happens, so the engine records 1 and then 2; a driver that
    * collects the yields first and converts them afterwards sees the same
    * object twice and records 2 twice.
    */
  private val mutatingYieldOpDesc: PythonOperatorDescriptor = inlinePyOp(
    name = "Mutating Yield",
    desc = "yields one dict twice, changing it in between",
    outName = "x",
    outType = AttributeType.INTEGER,
    bodyLines = Seq("""row = {"x": 1}""", "yield row", """row["x"] = 2""", "yield row"),
    tableOp = true
  )

  /** Reports the column names of the table it was handed, as one row. */
  private val tableColumnsOpDesc: PythonOperatorDescriptor = inlinePyOp(
    name = "Table Columns",
    desc = "yields the column names of its input table",
    outName = "columns",
    outType = AttributeType.STRING,
    bodyLines = Seq("""yield {"columns": ",".join(table.columns)}"""),
    tableOp = true
  )

  private def kindOp(desc: String, expr: String): PythonOperatorDescriptor =
    inlinePyOp(
      name = "Binary Type",
      desc = desc,
      outName = "kind",
      outType = AttributeType.STRING,
      bodyLines = Seq(s"""yield {"kind": $expr}""")
    )

  /** Declares one INTEGER column `x` and yields `row`, a Python dict literal. */
  private def yieldOp(row: String): PythonOperatorDescriptor =
    inlinePyOp(
      name = "Yield",
      desc = "yields one row as given",
      outName = "x",
      outType = AttributeType.INTEGER,
      bodyLines = Seq(s"yield $row")
    )

  /** Reads a LARGE_BINARY column and writes out the reference it holds.
    *
    * The bytes of a large binary live in S3 and the field is the s3:// URI that
    * points at them, so an operator can be handed one, and can hand one back,
    * without any storage being reachable.
    */
  private def largeBinaryOp(readsInput: Boolean): PythonOperatorDescriptor =
    inlinePyOp(
      name = "Large Binary",
      desc = "reads or writes a reference to a large binary",
      outName = if (readsInput) "uri" else "blob",
      outType = if (readsInput) AttributeType.STRING else AttributeType.LARGE_BINARY,
      bodyLines = Seq(
        if (readsInput) """yield {"uri": tuple_["blob"].uri}"""
        else """yield {"blob": largebinary(tuple_["uri"])}"""
      )
    )

  // A fitted DecisionTreeClassifier behind the marker the cast writes, pickled
  // by the interpreter the harness runs so the two share a sklearn. The script
  // goes through a file because Windows drops the quotes from a `-c` argument.
  private def fittedTreeCell(): Array[Byte] = {
    val script = Files.createTempFile("py-op-harness-model-", ".py")
    script.toFile.deleteOnExit()
    Files.writeString(
      script,
      """import base64, pickle, sys
        |from sklearn.tree import DecisionTreeClassifier
        |model = DecisionTreeClassifier(random_state=0).fit([[0.0], [1.0]], [0, 1])
        |sys.stdout.write(base64.b64encode(b"pickle    " + pickle.dumps(model)).decode("ascii"))
        |""".stripMargin
    )
    val encoded = Process(Seq(PyOpExecHarness.resolvePython(), script.toString)).!!
    Base64.getDecoder.decode(encoded.trim)
  }

  private def writeInputPort0(
      prefix: String,
      inputSchema: Schema,
      rows: Seq[Tuple]
  ): (Path, Path) = {
    val dir = Files.createTempDirectory(prefix)
    val input = dir.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(input, rows.iterator, inputSchema)
    (dir, input)
  }

  private def singleColRow(col: String, tpe: AttributeType, value: Any): (Schema, Seq[Tuple]) = {
    val schema = Schema().add(new Attribute(col, tpe))
    val row = Tuple.builder(schema).add(col, tpe, value).build()
    (schema, Seq(row))
  }

  private def runSinglePort[T](
      prefix: String,
      inputSchema: Schema,
      rows: Seq[Tuple],
      opDesc: PythonOperatorDescriptor,
      readField: Tuple => T
  ): Seq[T] = {
    val (dir, input) = writeInputPort0(prefix, inputSchema, rows)
    val result = PyOpExecHarness.execute(
      opDesc,
      inputs = Map(PortIdentity(0) -> input),
      outputDir = dir.resolve("actual")
    )
    val out = result.outputs(PortIdentity(0))
    TupleIO.readTuples(out, TupleIO.readSchemaSidecar(out)).map(readField).toSeq
  }

  private def runSingleCell[T](
      prefix: String,
      col: String,
      tpe: AttributeType,
      value: Any,
      opDesc: PythonOperatorDescriptor,
      readField: Tuple => T
  ): Seq[T] = {
    val (schema, rows) = singleColRow(col, tpe, value)
    runSinglePort(prefix, schema, rows, opDesc, readField)
  }

  // A model column arrives as the marker followed by the pickle, and the worker
  // unpickles it before the operator sees it, so predict works on it.
  "PyOpExecHarness" should "hand a pickled model cell to the operator as the model" in {
    val modelPredictOp = kindOp(
      "calls predict on a binary model cell",
      """str(tuple_["blob"].predict([[1.0]])[0])"""
    )
    runSingleCell(
      "py-op-harness-binary-",
      "blob",
      AttributeType.BINARY,
      fittedTreeCell(),
      modelPredictOp,
      _.getField[String]("kind")
    ).head shouldBe "1"
  }

  it should "hand any other binary cell to the operator as bytes" in {
    val binaryTypeOp =
      kindOp("reports the type of a binary cell", """type(tuple_["blob"]).__name__""")
    runSingleCell(
      "py-op-harness-binary-",
      "blob",
      AttributeType.BINARY,
      Array[Byte](0, 1, 2),
      binaryTypeOp,
      _.getField[String]("kind")
    ).head shouldBe "bytes"
  }

  // The worker gets the wall clock as a zoneless Arrow microsecond timestamp,
  // so the operator holds a naive datetime cut to the microsecond, and a year
  // past pandas' nanosecond range reaches it all the same.
  it should "hand a timestamp to the operator as the worker hands it" in {
    val timestampTextOp = kindOp("reports a timestamp cell", """str(tuple_["ts"])""")
    runSingleCell(
      "py-op-harness-timestamp-",
      "ts",
      AttributeType.TIMESTAMP,
      Timestamp.valueOf("2500-01-01 00:00:00.123456789"),
      timestampTextOp,
      _.getField[String]("kind")
    ) shouldBe Seq("2500-01-01 00:00:00.123456")
  }

  private val someObject = "s3://a-bucket/a/large/object"

  // The schema sidecar names large_binary, so the harness has to carry it in as
  // well as out. The operator reads the reference itself, which a column handed
  // over as plain text could not answer for.
  "PyOpExecHarness" should "hand a large binary column to the operator as a reference" in {
    runSingleCell(
      "py-op-harness-large-binary-in-",
      "blob",
      AttributeType.LARGE_BINARY,
      new LargeBinary(someObject),
      largeBinaryOp(readsInput = true),
      _.getField[String]("uri")
    ) shouldBe Seq(someObject)
  }

  it should "write back a large binary the operator made" in {
    // Read back as a LargeBinary and not as the text of one: the column is
    // declared large_binary, and this is the type the rest of the pipeline gets.
    runSingleCell(
      "py-op-harness-large-binary-out-",
      "uri",
      AttributeType.STRING,
      someObject,
      largeBinaryOp(readsInput = false),
      _.getField[LargeBinary]("blob").getUri
    ) shouldBe Seq(someObject)
  }

  // The worker finalizes each yielded row against the output schema, and so
  // does the driver: a value the cast cannot bring to the declared type, and a
  // field the schema does not have or lacks, end the run in both.
  "PyOpExecHarness" should "refuse a row the output schema does not describe" in {
    val (inputSchema, rows) = singleColRow("seed", AttributeType.INTEGER, Int.box(1))
    val (dir, input) = writeInputPort0("py-op-harness-finalize-", inputSchema, rows)

    def run(row: String): Unit =
      PyOpExecHarness.execute(
        yieldOp(row),
        inputs = Map(PortIdentity(0) -> input),
        outputDir = dir.resolve("actual")
      )

    noException should be thrownBy run("""{"x": 1}""")
    Seq("""{"x": 1.5}""", """{"x": 1, "y": 2}""", "{}").foreach { row =>
      withClue(s"yielding $row: ") {
        a[PyOpDriverException] should be thrownBy run(row)
      }
    }
  }

  "PyOpExecHarness" should "record each yield as it was yielded" in {
    val (inputSchema, rows) = singleColRow("seed", AttributeType.INTEGER, Int.box(1))
    val values = runSinglePort(
      "py-op-harness-",
      inputSchema,
      rows,
      mutatingYieldOpDesc,
      _.getField[Integer]("x")
    )
    withClue(s"schema was $inputSchema\n") {
      values shouldBe Seq(1, 2)
    }
  }

  // A port that carried no rows gives the table no tuples to read column names
  // off, so the worker hands the operator the port's declared schema instead.
  "PyOpExecHarness" should "hand a port that carried no rows its declared columns" in {
    val inputSchema = Schema()
      .add(new Attribute("name", AttributeType.STRING))
      .add(new Attribute("n", AttributeType.INTEGER))
    runSinglePort(
      "py-op-harness-empty-",
      inputSchema,
      Seq.empty,
      tableColumnsOpDesc,
      _.getField[String]("columns")
    ) shouldBe Seq("name,n")
  }
}
