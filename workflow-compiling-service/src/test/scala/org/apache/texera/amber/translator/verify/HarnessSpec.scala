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
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp, PortIdentity}
import org.apache.texera.amber.operator.distinct.DistinctOpDesc
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.scalatest.Tag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/** The two ways of running one operator, and the file format they meet in.
  *
  * `Distinct` is the operator under test throughout, because what is being
  * tested is the harness rather than the operator: it takes one input, needs no
  * configuration, and its answer is short enough to state in full.
  */
class HarnessSpec extends AnyFlatSpec with Matchers {

  /** Only the standalone run needs an interpreter, so only it is held back from
    * the job that provisions none. The other two are JVM-side and run there.
    */
  private val NeedsPython =
    Tag("org.apache.texera.amber.translator.verify.tags.IntegrationTest")

  private val schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING)
  )

  private def tuple(id: Int, name: String): Tuple = {
    val b = Tuple.builder(schema)
    b.add(schema.getAttribute("id"), Int.box(id))
    b.add(schema.getAttribute("name"), name)
    b.build()
  }

  /** Four rows, the last a repeat of the second. */
  private val rows = Seq(tuple(1, "a"), tuple(2, "b"), tuple(3, "c"), tuple(2, "b"))

  private def withInput(test: (Path, Path) => Unit): Unit = {
    val dir = Files.createTempDirectory("harness-spec-")
    val input = dir.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(input, rows.iterator, schema)
    test(dir, input)
  }

  /** A source, in the one respect this spec is about: it reads no input port and
    * names the file it reads by placeholder, leaving the naming to whoever
    * assembles the script.
    */
  private class StubSource(path: Path) extends LogicalOp with StandaloneCodeGenerator {
    override def getPhysicalOp(
        workflowId: WorkflowIdentity,
        executionId: ExecutionIdentity
    ): PhysicalOp =
      throw new UnsupportedOperationException("the harness never builds a physical op")

    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "StubSource",
        "Stands in for a source that names its file by placeholder",
        OperatorGroupConstants.INPUT_GROUP,
        inputPorts = List.empty,
        outputPorts = List(OutputPort())
      )

    override def standaloneSourcePath(): Option[String] = Some(path.toUri.toString)

    override def generateStandaloneCode(): String =
      s"out1df = pd.read_json(${StandaloneCodeGenerator.SourceFilePlaceholder}, lines=True)"
  }

  /** Reports the Python type of each cell of its `blob` column. */
  private class BlobTypeOp extends LogicalOp with StandaloneCodeGenerator {
    override def getPhysicalOp(
        workflowId: WorkflowIdentity,
        executionId: ExecutionIdentity
    ): PhysicalOp =
      throw new UnsupportedOperationException("the harness never builds a physical op")

    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "BlobType",
        "Reports the type of each binary cell",
        OperatorGroupConstants.UTILITY_GROUP,
        inputPorts = List(InputPort()),
        outputPorts = List(OutputPort())
      )

    override def generateStandaloneCode(): String =
      """out1df = pd.DataFrame({"kind": [type(_v).__name__ for _v in in1df["blob"]]})"""
  }

  private def blobKind(cell: Array[Byte]): String = {
    val blobOnly = new Schema(new Attribute("blob", AttributeType.BINARY))
    val dir = Files.createTempDirectory("harness-spec-blob-kind-")
    val input = dir.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(
      input,
      Iterator(Tuple.builder(blobOnly).add(blobOnly.getAttribute("blob"), cell).build()),
      blobOnly
    )
    val result = StandaloneRunner.run(
      opDesc = new BlobTypeOp,
      inputs = Map(1 -> input),
      outputPortCount = 1,
      workDir = dir
    )
    val lines = Files.readAllLines(result.outputs(1))
    lines should have size 1
    lines.get(0)
  }

  "TupleIO" should "read back the rows and the schema it wrote" in {
    withInput { (_, input) =>
      // The schema travels in a sidecar rather than in the JSONL, which carries
      // values alone and so cannot say a column is INTEGER rather than a number.
      TupleIO.readSchemaSidecar(input) shouldBe schema
      val read = TupleIO.readTuples(input, schema).toSeq
      read should have length 4
      read.map(_.getField[Integer]("id").intValue) shouldBe Seq(1, 2, 3, 2)
    }
  }

  "OpExecHarness" should "run an operator and write one file per output port" in {
    withInput { (dir, input) =>
      val out = dir.resolve("actual")
      val result =
        OpExecHarness.execute(new DistinctOpDesc, Map(PortIdentity(0) -> input), out)

      result.outputs should have size 1
      val produced = result.outputs(PortIdentity(0))
      Files.exists(produced) shouldBe true

      val written = TupleIO.readTuples(produced, result.outputSchemas(PortIdentity(0))).toSeq
      written.map(_.getField[Integer]("id").intValue) shouldBe Seq(1, 2, 3)
    }
  }

  "StandaloneRunner" should "run the generated script and reach the same answer" taggedAs NeedsPython in {
    withInput { (dir, input) =>
      val work = dir.resolve("standalone")
      Files.createDirectories(work)
      val result = StandaloneRunner.run(
        opDesc = new DistinctOpDesc,
        inputs = Map(1 -> input),
        outputPortCount = 1,
        workDir = work
      )

      // The script is kept where it ran, so a failing operator can be opened as
      // generated rather than described second-hand.
      Files.exists(work.resolve("script.py")) shouldBe true

      val produced = result.outputs(1)
      val lines = Files.readAllLines(produced)
      lines should have size 3
      lines.get(0) should include("\"id\":1")
      lines.get(2) should include("\"id\":3")
    }
  }

  // A source writes a placeholder where its file should be named, since only
  // whoever assembles the whole script can settle on a name. Nothing bound it
  // here, so every source's script stopped on a `sourceFile` that was never
  // defined, which no operator spec could see: they assert the text the operator
  // emits, and the text is right.
  it should "name the file a source reads, which the body leaves to it" taggedAs NeedsPython in {
    val dir = Files.createTempDirectory("harness-source-")
    // Beside the script, under the name the source offers, which is how an
    // exported script is meant to find its data.
    val data = dir.resolve("rows.jsonl")
    TupleIO.writeTuples(data, rows.iterator, schema)

    val result = StandaloneRunner.run(
      opDesc = new StubSource(data),
      inputs = Map.empty,
      outputPortCount = 1,
      workDir = dir
    )

    val script = Files.readString(dir.resolve("script.py"))
    script should include("sourceFile = 'rows.jsonl'")

    val lines = Files.readAllLines(result.outputs(1))
    lines should have size 4
    lines.get(0) should include("\"id\":1")
    lines.get(3) should include("\"id\":2")
  }

  // pandas has no plain boolean column that carries a null, so read_json reads
  // one with a hole as float64 and the operator is handed 1.0 and 0.0 where the
  // run had true and false. The prologue takes the column back to the nullable
  // boolean dtype, which carries the two values and the hole.
  it should "hand a boolean column with a hole to the script as booleans" taggedAs NeedsPython in {
    val holed = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("flag", AttributeType.BOOLEAN)
    )
    def row(id: Int, flag: java.lang.Boolean): Tuple = {
      val b = Tuple.builder(holed)
      b.add(holed.getAttribute("id"), Int.box(id))
      b.add(holed.getAttribute("flag"), flag)
      b.build()
    }

    val dir = Files.createTempDirectory("harness-spec-boolean-")
    val input = dir.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(input, Iterator(row(1, true), row(2, null), row(3, false)), holed)
    val work = dir.resolve("standalone")
    Files.createDirectories(work)

    val result = StandaloneRunner.run(
      opDesc = new DistinctOpDesc,
      inputs = Map(1 -> input),
      outputPortCount = 1,
      workDir = work
    )

    val lines = Files.readAllLines(result.outputs(1))
    lines should have size 3
    lines.get(0) should include("\"flag\":true")
    lines.get(1) should include("\"flag\":null")
    lines.get(2) should include("\"flag\":false")
  }

  // The engine writes a tuple through the schema, so a column it declares
  // INTEGER leaves as an integer. pandas has no plain integer that carries a
  // null, so the same column left the script as 6.0 as soon as a row was
  // missing, and the two sides disagreed on every value in it.
  it should "write an integral column with a hole the way the engine writes it" taggedAs NeedsPython in {
    val holed = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("n", AttributeType.INTEGER)
    )
    def row(id: Int, n: Integer): Tuple = {
      val b = Tuple.builder(holed)
      b.add(holed.getAttribute("id"), Int.box(id))
      b.add(holed.getAttribute("n"), n)
      b.build()
    }

    val dir = Files.createTempDirectory("harness-spec-integral-")
    val input = dir.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(input, Iterator(row(1, 6), row(2, null), row(3, 7)), holed)
    val work = dir.resolve("standalone")
    Files.createDirectories(work)

    val result = StandaloneRunner.run(
      opDesc = new DistinctOpDesc,
      inputs = Map(1 -> input),
      outputPortCount = 1,
      workDir = work,
      outputSchemas = Map(PortIdentity(0) -> holed)
    )

    val lines = Files.readAllLines(result.outputs(1))
    lines should have size 3
    lines.get(0) should include("\"n\":6")
    lines.get(0) should not include "\"n\":6.0"
    lines.get(1) should include("\"n\":null")
    lines.get(2) should include("\"n\":7")
  }

  // JSON carries bytes as base64 text, and the engine decodes it before the
  // operator sees the field (TupleIO.readTuples). Left as text, the script's
  // operator is handed a str where the run's was handed bytes, and anything it
  // does with them answers for the base64 rather than for the value.
  it should "hand a binary column to the script as bytes" taggedAs NeedsPython in {
    val withBlob = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("blob", AttributeType.BINARY)
    )
    def row(id: Int, blob: Array[Byte]): Tuple = {
      val b = Tuple.builder(withBlob)
      b.add(withBlob.getAttribute("id"), Int.box(id))
      b.add(withBlob.getAttribute("blob"), blob)
      b.build()
    }

    val dir = Files.createTempDirectory("harness-spec-binary-")
    val input = dir.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(
      input,
      Iterator(row(1, "hi".getBytes(StandardCharsets.UTF_8)), row(2, null)),
      withBlob
    )
    val work = dir.resolve("standalone")
    Files.createDirectories(work)

    val result = StandaloneRunner.run(
      opDesc = new DistinctOpDesc,
      inputs = Map(1 -> input),
      outputPortCount = 1,
      workDir = work
    )

    // The prologue decodes that column and no other. Distinct answers the same
    // whether it is handed the bytes or the base64 spelling of them, so what the
    // body is given has to be read off the script rather than off the output.
    val script = Files.readString(work.resolve("script.py"))
    script should include("in1df['blob'] = in1df['blob'].map(")
    script should include("base64.b64decode")
    script should not include "in1df['id'] = in1df['id'].map("

    // And the column still leaves as the base64 the engine's writer wrote:
    // decoding it on the way in must not turn it into a pickle on the way out.
    val lines = Files.readAllLines(result.outputs(1))
    lines should have size 2
    lines.get(0) should include("\"blob\":\"aGk=\"")
    lines.get(1) should include("\"blob\":null")
  }

  // A model column arrives as the marker followed by the pickle, and the worker
  // unpickles it before the operator sees it. This is pickle.dumps(["a"],
  // protocol=0), which stands in for a fitted estimator.
  it should "hand a pickled binary cell to the script as the object" taggedAs NeedsPython in {
    val pickled = "pickle    ".getBytes("US-ASCII") ++ "(lp0\nVa\np1\na.".getBytes("US-ASCII")
    blobKind(pickled) should include("\"kind\":\"list\"")
  }

  it should "hand any other binary cell to the script as bytes" taggedAs NeedsPython in {
    blobKind(Array[Byte](0, 1, 2)) should include("\"kind\":\"bytes\"")
  }
}
