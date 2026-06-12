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

import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.source.fetcher.URLFetcherOpDesc
import org.apache.texera.amber.operator.source.scan.arrow.ArrowSourceOpDesc
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.csvOld.CSVOldScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.file.{FileScanOpDesc, FileScanSourceOpDesc}
import org.apache.texera.amber.operator.source.scan.json.JSONLScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema => ArrowSchema}
import org.apache.arrow.vector.{IntVector, VarCharVector, VectorSchemaRoot}

import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.util.Using

/**
  * Per-category test runner for source operators (operators with no input
  * ports — they read from an external resource and emit tuples).
  *
  * Adding a new source operator to Texera should yield free behavioral
  * verification: enumerate via [[LogicalOp]]'s `@JsonSubTypes`, dispatch
  * here if the operator's class is in [[handlers]], otherwise mark the
  * test as ignored. Adding support for a new source format (parquet, ORC,
  * a SQL adapter) means writing one [[SourceHandler]] object — the only
  * file in the test tree that needs editing.
  *
  * Each handler is responsible for two things:
  *   1. Generating a sample file the operator can read (CSV bytes, JSONL
  *      bytes, Arrow stream, …). The file lives in `testRoot`.
  *   2. Returning a fully-configured OpDesc instance pointing at that file
  *      with all required fields populated.
  *
  * The runner itself is operator-agnostic: it asks the handler for an
  * OpDesc, drives [[OpExecHarness]] (Path A) and [[StandaloneRunner]]
  * (Path B), compares via [[Comparator]]. Sources have no input ports so
  * `inputs = Map.empty` for both paths.
  */
object SourceCategoryRunner {

  /** Maps an OpDesc class to the handler that knows how to fixture it. */
  private val handlersByClass: Map[Class[_ <: LogicalOp], SourceHandler] =
    Seq[SourceHandler](
      CsvScanHandler,
      CsvOldScanHandler,
      JsonlScanHandler,
      TextInputHandler,
      FileScanSourceHandler,
      ArrowScanHandler
    ).map(h => h.opDescClass -> h).toMap

  /**
    * Sources this runner cannot verify, with the honest reason. Mirrors
    * `TransformVerificationRunner.knownIssues`: the reason surfaces in the
    * ignored test's name and the coverage table.
    */
  private val knownIssues: Map[Class[_ <: LogicalOp], String] = Map(
    classOf[FileScanOpDesc] ->
      ("input-driven source: filenames arrive on an input port at runtime, but this runner " +
        "feeds sources no inputs — Path B's generated code references an undefined in1df"),
    classOf[URLFetcherOpDesc] ->
      ("generated code needs urllib.request, which the translator's shared imports lack " +
        "(NameError in Path B); verifying would also depend on a live network fetch")
  )

  def canRun(opDescClass: Class[_ <: LogicalOp]): Boolean =
    handlersByClass.contains(opDescClass)

  /** Why a non-runnable source is flagged (specific known issue, else no handler yet). */
  def flagReason(opDescClass: Class[_ <: LogicalOp]): String =
    knownIssues.getOrElse(opDescClass, "no source handler registered yet")

  /** Runs the parity test for the operator. Throws on mismatch. */
  def run(opDescClass: Class[_ <: LogicalOp]): Unit = {
    val handler = handlersByClass.getOrElse(
      opDescClass,
      throw new IllegalArgumentException(
        s"No SourceHandler registered for ${opDescClass.getSimpleName}. " +
          s"Add one to SourceCategoryRunner.handlersByClass."
      )
    )

    val testRoot = Files.createTempDirectory(s"op-behavior-${opDescClass.getSimpleName}-")
    val opDesc = handler.makeOpDesc(testRoot)

    val actualDir = testRoot.resolve("actual")
    Files.createDirectories(actualDir)

    val pathA = OpExecHarness.execute(opDesc, inputs = Map.empty, outputDir = actualDir)
    val pathB = StandaloneRunner.run(
      opDesc = opDesc,
      inputs = Map.empty,
      outputPortCount = 1,
      workDir = testRoot
    )

    val actual = pathA.outputs(PortIdentity(0))
    val expected = pathB.outputs(1)
    Comparator.assertEqual(actual, expected)
  }
}

/**
  * One source operator's recipe: which OpDesc class it handles, how to
  * fixture a working instance of it. Operators in the same family share
  * a handler (e.g. all CSV variants reuse [[CsvScanHandler]]'s sample
  * file, even if they have different OpDesc classes).
  */
trait SourceHandler {

  /** The concrete OpDesc class this handler tests. */
  def opDescClass: Class[_ <: LogicalOp]

  /**
    * Generate the fixture file inside `testRoot` and return a configured
    * OpDesc instance whose `fileName` (or analogous URI field) points at it.
    */
  def makeOpDesc(testRoot: Path): LogicalOp
}

/**
  * Handler for `CSVScanSourceOpDesc`. Writes a 3-row CSV with an integer
  * column and a string column — enough to exercise the read + type
  * inference + header parsing in both paths.
  */
object CsvScanHandler extends SourceHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[CSVScanSourceOpDesc]

  override def makeOpDesc(testRoot: Path): LogicalOp = {
    val csvPath = testRoot.resolve("sample.csv")
    val csvContent =
      """id,name
        |1,alice
        |2,bob
        |3,carol
        |""".stripMargin
    Files.write(csvPath, csvContent.getBytes(StandardCharsets.UTF_8))

    val desc = new CSVScanSourceOpDesc()
    desc.fileName = Some(csvPath.toUri.toString)
    desc.customDelimiter = Some(",")
    desc.hasHeader = true
    desc
  }
}

/** Handler for `CSVOldScanSourceOpDesc`. Same fixture shape as [[CsvScanHandler]]. */
object CsvOldScanHandler extends SourceHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[CSVOldScanSourceOpDesc]

  override def makeOpDesc(testRoot: Path): LogicalOp = {
    val csvPath = testRoot.resolve("sample.csv")
    val csvContent =
      """id,name
        |1,alice
        |2,bob
        |3,carol
        |""".stripMargin
    Files.write(csvPath, csvContent.getBytes(StandardCharsets.UTF_8))

    val desc = new CSVOldScanSourceOpDesc()
    desc.fileName = Some(csvPath.toUri.toString)
    desc.customDelimiter = Some(",")
    desc.hasHeader = true
    desc
  }
}

/**
  * Handler for `JSONLScanSourceOpDesc`. Keys are in alphabetical order so both
  * paths agree on column order: Texera sorts inferred field names, while
  * pandas keeps the first record's key order.
  */
object JsonlScanHandler extends SourceHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[JSONLScanSourceOpDesc]

  override def makeOpDesc(testRoot: Path): LogicalOp = {
    val jsonlPath = testRoot.resolve("sample.jsonl")
    val jsonlContent =
      """{"id": 1, "name": "alice"}
        |{"id": 2, "name": "bob"}
        |{"id": 3, "name": "carol"}
        |""".stripMargin
    Files.write(jsonlPath, jsonlContent.getBytes(StandardCharsets.UTF_8))

    val desc = new JSONLScanSourceOpDesc()
    desc.fileName = Some(jsonlPath.toUri.toString)
    desc.flatten = false
    desc
  }
}

/** Handler for `TextInputSourceOpDesc`. The text lives in the config — no fixture file. */
object TextInputHandler extends SourceHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[TextInputSourceOpDesc]

  override def makeOpDesc(testRoot: Path): LogicalOp = {
    val desc = new TextInputSourceOpDesc()
    desc.textInput = "alice\nbob\ncarol"
    desc // defaults: attributeType STRING (one row per line), attributeName "line"
  }
}

/** Handler for `FileScanSourceOpDesc`. Plain text file read in default line mode. */
object FileScanSourceHandler extends SourceHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[FileScanSourceOpDesc]

  override def makeOpDesc(testRoot: Path): LogicalOp = {
    val txtPath = testRoot.resolve("sample.txt")
    Files.write(txtPath, "alice\nbob\ncarol\n".getBytes(StandardCharsets.UTF_8))

    val desc = new FileScanSourceOpDesc()
    desc.fileName = Some(txtPath.toUri.toString)
    desc // defaults: attributeType STRING (one row per line), attributeName "line"
  }
}

/**
  * Handler for `ArrowSourceOpDesc`. Writes a 3-row Arrow IPC file (the
  * uncompressed "file" format) with the Java Arrow API — the same format
  * `ArrowFileReader` (Path A) and `pd.read_feather` (Path B) both read.
  */
object ArrowScanHandler extends SourceHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[ArrowSourceOpDesc]

  override def makeOpDesc(testRoot: Path): LogicalOp = {
    val arrowPath = testRoot.resolve("sample.arrow")
    val arrowSchema = new ArrowSchema(
      java.util.Arrays.asList(
        new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
      )
    )

    Using.Manager { use =>
      val allocator = use(new RootAllocator())
      val root = use(VectorSchemaRoot.create(arrowSchema, allocator))
      root.allocateNew()
      val ids = root.getVector("id").asInstanceOf[IntVector]
      val names = root.getVector("name").asInstanceOf[VarCharVector]
      Seq(1, 2, 3).zipWithIndex.foreach { case (v, i) => ids.setSafe(i, v) }
      Seq("alice", "bob", "carol").zipWithIndex.foreach {
        case (v, i) => names.setSafe(i, v.getBytes(StandardCharsets.UTF_8))
      }
      root.setRowCount(3)

      val channel = use(
        FileChannel.open(arrowPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      )
      val writer = use(new ArrowFileWriter(root, null, channel))
      writer.start()
      writer.writeBatch()
      writer.end()
    }.get

    val desc = new ArrowSourceOpDesc()
    desc.fileName = Some(arrowPath.toUri.toString)
    desc
  }
}
