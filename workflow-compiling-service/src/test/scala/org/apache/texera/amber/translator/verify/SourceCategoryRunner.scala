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
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.source.fetcher.URLFetcherOpDesc
import org.apache.texera.amber.operator.source.scan.ScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.file.{FileScanOpDesc, FileScanSourceOpDesc}
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema => ArrowSchema}
import org.apache.arrow.vector.{Float8Vector, IntVector, VarCharVector, VectorSchemaRoot}

import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

/**
  * Per-category test runner for source operators (operators with no input
  * ports — they read from an external resource and emit tuples).
  *
  * Dispatch is auto-first:
  *   - Auto tier: a scan source declares the file format it reads via
  *     [[ScanSourceOpDesc.fileTypeName]]. If that tag is in [[encoderByFileType]],
  *     the operator is fixtured with zero per-operator code — the shared
  *     [[CanonicalSourceFixture]] is encoded into that format and `fileName`
  *     points at it. A newly added file-scan source in a known format
  *     (CSV/JSONL/Arrow/…) is verified the moment it is registered in
  *     [[LogicalOp]]'s `@JsonSubTypes`, no edit here.
  *   - Curated tier: sources that can't take the shared table (text-family
  *     single-`line` output, or inline-config data) keep a hand-written
  *     [[SourceHandler]] in [[curatedHandlersByClass]].
  *   - Otherwise the test is flagged (a [[knownIssues]] reason, an unsupported
  *     declared format, or no match) — never silently skipped.
  *
  * The runner itself is operator-agnostic: it builds an OpDesc, drives
  * [[OpExecHarness]] (Path A) and [[StandaloneRunner]] (Path B), compares via
  * [[Comparator]]. Sources have no input ports so `inputs = Map.empty` for both.
  */
object SourceCategoryRunner {

  /**
    * The curated tier: sources that keep a hand-written handler because they
    * can't go through the shared-fixture + encoder (auto) path — their output
    * isn't the shared 3-column table (text-family, single `line` column) or
    * their data is inline config rather than a file. Mirrors the transform
    * side's [[CuratedHandlers]] (hand-written vs auto-generated fixture).
    */
  private val curatedHandlersByClass: Map[Class[_ <: LogicalOp], SourceHandler] =
    Seq[SourceHandler](TextInputHandler, FileScanSourceHandler)
      .map(h => h.opDescClass -> h)
      .toMap

  /**
    * The auto tier. A scan source declares the file format it reads via
    * [[ScanSourceOpDesc.fileTypeName]] ("CSV", "JSONL", "Arrow", …). Map that
    * tag to the [[CanonicalSourceFixture]] encoder that writes a file in that
    * format. Any source whose `fileTypeName` is a key here runs with zero
    * per-operator code, so a newly added file-scan source in a known format is
    * verified the moment it is registered in `@JsonSubTypes` — no handler, no
    * edit here. (ParallelCSV also declares "CSV" and would be covered for free,
    * but it is currently commented out of `@JsonSubTypes`, so the suite doesn't
    * enumerate it.)
    */
  private val encoderByFileType: Map[String, Path => Path] = Map(
    "CSV" -> CanonicalSourceFixture.writeCsv,
    "CSVOld" -> CanonicalSourceFixture.writeCsv,
    "JSONL" -> CanonicalSourceFixture.writeJsonl,
    "Arrow" -> CanonicalSourceFixture.writeArrow
  )

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

  /** The format tag a source declares, or `None` if it isn't an instantiable
    * ScanSourceOpDesc (non-scan sources, or ones that fail to construct). */
  private def declaredFileType(opDescClass: Class[_ <: LogicalOp]): Option[String] =
    Try(opDescClass.getDeclaredConstructor().newInstance()).toOption.collect {
      case scan: ScanSourceOpDesc => scan.fileTypeName
    }.flatten

  def canRun(opDescClass: Class[_ <: LogicalOp]): Boolean =
    curatedHandlersByClass.contains(opDescClass) ||
      declaredFileType(opDescClass).exists(encoderByFileType.contains)

  /**
    * Tier label for a runnable source, mirroring the transform side's
    * auto/curated distinction: `"curated source"` when a hand-written
    * [[SourceHandler]] serves it, else `"auto source"` (a declared-format scan
    * source fixtured by an [[encoderByFileType]] encoder with zero per-op code).
    */
  def tier(opDescClass: Class[_ <: LogicalOp]): String =
    if (curatedHandlersByClass.contains(opDescClass)) "curated source" else "auto source"

  /** Why a non-runnable source is flagged: a specific known issue, an
    * unsupported declared format, or no handler/format match at all. */
  def flagReason(opDescClass: Class[_ <: LogicalOp]): String =
    knownIssues.getOrElse(
      opDescClass,
      declaredFileType(opDescClass) match {
        case Some(fileType) =>
          s"unsupported source format '$fileType' — no encoder registered in SourceCategoryRunner"
        case None => "no source handler registered yet"
      }
    )

  /**
    * Build the configured OpDesc: a hand-written curated handler if one is
    * registered, otherwise the auto path — instantiate the operator, encode the
    * shared fixture in the format it declares, and point `fileName` at the file.
    */
  private def makeOpDesc(opDescClass: Class[_ <: LogicalOp], testRoot: Path): LogicalOp =
    curatedHandlersByClass.get(opDescClass) match {
      case Some(handler) => handler.makeOpDesc(testRoot)
      case None =>
        val scan = opDescClass.getDeclaredConstructor().newInstance() match {
          case s: ScanSourceOpDesc => s
          case other =>
            throw new IllegalArgumentException(
              s"${opDescClass.getSimpleName} has no curated handler and is not a " +
                s"ScanSourceOpDesc (${other.getClass.getName})"
            )
        }
        val fileType = scan.fileTypeName.getOrElse("")
        val encoder = encoderByFileType.getOrElse(
          fileType,
          throw new IllegalArgumentException(
            s"No encoder for ${opDescClass.getSimpleName} (fileTypeName='$fileType')"
          )
        )
        scan.fileName = Some(encoder(testRoot).toUri.toString)
        scan
    }

  /** Runs the parity test for the operator. Throws on mismatch. */
  def run(opDescClass: Class[_ <: LogicalOp]): Unit = {
    val testRoot = Files.createTempDirectory(s"op-behavior-${opDescClass.getSimpleName}-")
    val opDesc = makeOpDesc(opDescClass, testRoot)

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
  * A hand-written recipe for one source that can't use the auto tier
  * (fileTypeName + [[CanonicalSourceFixture]] encoder): which OpDesc class it
  * handles and how to fixture a working instance. Used for the text-family
  * sources ([[TextInputHandler]], [[FileScanSourceHandler]]).
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
  * The shared row data every structured-file source reads. Mirrors
  * [[CanonicalFixture]] / [[SklearnFixture]]: the rows live in a checked-in,
  * human-readable JSON resource (source_fixture.json) and are loaded once here.
  *
  * A source has no input port, so the fixture is delivered not as an input
  * JSONL but as a file the operator opens itself. Each `writeXxx` encodes the
  * same rows into one on-disk format (CSV / JSONL / Arrow); a source handler
  * picks the encoder its operator understands and points `fileName` at the
  * result. So CSV, CSVOld, JSONL and Arrow all verify that the operator can
  * reconstruct one shared 3-column table — instead of each asserting against
  * its own ad-hoc sample.
  *
  * Types (id INTEGER, name STRING, score DOUBLE) round-trip cleanly across all
  * three formats and are inferred identically by Texera (Path A) and pandas
  * (Path B): every `name` is non-numeric and every `score` carries a decimal,
  * so neither column is mis-inferred as numeric/integer.
  */
object CanonicalSourceFixture {

  val schema: Schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING),
    new Attribute("score", AttributeType.DOUBLE)
  )

  private val fixtureResource = "/verify/source_fixture.json"

  val rows: Vector[Tuple] = {
    val stream = Option(getClass.getResourceAsStream(fixtureResource))
      .getOrElse(sys.error(s"source fixture not found on classpath: $fixtureResource"))
    val root =
      try new ObjectMapper().readTree(stream)
      finally stream.close()
    root.elements().asScala.map { node =>
      val b = Tuple.builder(schema)
      schema.getAttributes.foreach { attr =>
        val cell = node.get(attr.getName)
        require(cell != null, s"source fixture row missing column '${attr.getName}'")
        val value: AnyRef = attr.getType match {
          case AttributeType.INTEGER => Int.box(cell.asInt())
          case AttributeType.DOUBLE  => Double.box(cell.asDouble())
          case _                     => cell.asText()
        }
        b.add(attr, value)
      }
      b.build()
    }.toVector
  }

  /** Write the rows as a header-first, comma-delimited CSV. */
  def writeCsv(dir: Path): Path = {
    val path = dir.resolve("sample.csv")
    val header = schema.getAttributes.map(_.getName).mkString(",")
    val body = rows.map { t =>
      schema.getAttributes.map(a => t.getField(a.getName).toString).mkString(",")
    }
    Files.write(
      path,
      ((header +: body).mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8)
    )
    path
  }

  /** Write the rows as JSON Lines (one object per line, keys in schema order).
    * Reuses [[TupleIO.writeTuples]] — the same writer the transform fixtures
    * use; it also drops a `.schema.json` sidecar the source ignores. */
  def writeJsonl(dir: Path): Path = {
    val path = dir.resolve("sample.jsonl")
    TupleIO.writeTuples(path, rows.iterator, schema)
    path
  }

  /** Write the rows as an uncompressed Arrow IPC ("file" format) stream — the
    * format both `ArrowFileReader` (Path A) and `pd.read_feather` (Path B)
    * read. */
  def writeArrow(dir: Path): Path = {
    val path = dir.resolve("sample.arrow")
    val arrowSchema = new ArrowSchema(
      java.util.Arrays.asList(
        new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null),
        new Field("name", FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
        new Field(
          "score",
          FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
          null
        )
      )
    )
    Using.Manager { use =>
      val allocator = use(new RootAllocator())
      val root = use(VectorSchemaRoot.create(arrowSchema, allocator))
      root.allocateNew()
      val ids = root.getVector("id").asInstanceOf[IntVector]
      val names = root.getVector("name").asInstanceOf[VarCharVector]
      val scores = root.getVector("score").asInstanceOf[Float8Vector]
      rows.zipWithIndex.foreach {
        case (t, i) =>
          ids.setSafe(i, t.getField("id").asInstanceOf[Int])
          names.setSafe(
            i,
            t.getField("name").asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
          )
          scores.setSafe(i, t.getField("score").asInstanceOf[Double])
      }
      root.setRowCount(rows.size)
      val channel = use(
        FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
      )
      val writer = use(new ArrowFileWriter(root, null, channel))
      writer.start()
      writer.writeBatch()
      writer.end()
    }.get
    path
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
