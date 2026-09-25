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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Types}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.source.fetcher.URLFetcherOpDesc
import org.apache.texera.amber.operator.source.scan.{FileAttributeType, ScanSourceOpDesc}
import org.apache.texera.amber.operator.source.scan.file.{FileScanOpDesc, FileScanSourceOpDesc}
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.arrow.vector.types.pojo.{Schema => ArrowSchema}
import org.apache.arrow.vector.{
  FieldVector,
  Float4Vector,
  SmallIntVector,
  TimeStampMilliTZVector,
  TinyIntVector,
  UInt1Vector,
  UInt2Vector,
  UInt4Vector,
  VectorSchemaRoot
}
import org.apache.texera.amber.util.ArrowUtils

import java.nio.channels.FileChannel
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path, StandardOpenOption}
import java.time.ZoneOffset
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

/**
  * Per-category test runner for source operators (operators with no input
  * ports — they read from an external resource and emit tuples).
  *
  * Dispatch is auto-first. A scan source declares the format it reads via
  * [[ScanSourceOpDesc.fileTypeName]], and where [[encoderByFileType]] knows that
  * format the shared [[CanonicalSourceFixture]] is encoded into it with no
  * per-operator code at all: a newly registered file-scan source in a known
  * format is verified the moment it appears in [[LogicalOp]]'s `@JsonSubTypes`.
  * A source that cannot take the shared table keeps a hand-written
  * [[SourceHandler]] instead: the text family emits a single `line` column, and
  * some carry their data inline. Anything else is flagged, never silently
  * skipped.
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
  private val encoderByFileType: Map[String, (Path, Charset, Seq[Tuple]) => Path] = Map(
    "CSV" -> CanonicalSourceFixture.writeCsv,
    "CSVOld" -> CanonicalSourceFixture.writeCsv,
    "JSONL" -> CanonicalSourceFixture.writeJsonl,
    // Arrow is binary and its descriptor declares fileEncoding ignored, so the
    // charset a variant asks for has nothing to apply to.
    "Arrow" -> ((dir, _, rows) => CanonicalSourceFixture.writeArrow(dir, rows)),
    "Parquet" -> ((dir, _, rows) => CanonicalSourceFixture.writeParquet(dir, rows))
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
      ("live-network source: the operator fetches a real URL over the network, so its " +
        "output is non-deterministic and depends on external connectivity — it cannot be " +
        "verified against a fixed fixture in isolation")
  )

  /** The format tag a source declares, or `None` if it isn't an instantiable
    * ScanSourceOpDesc (non-scan sources, or ones that fail to construct).
    */
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
    * unsupported declared format, or no handler/format match at all.
    */
  def flagReason(opDescClass: Class[_ <: LogicalOp]): String =
    knownIssues.getOrElse(
      opDescClass,
      declaredFileType(opDescClass) match {
        case Some(fileType) =>
          s"unsupported source format '$fileType' — no encoder registered in SourceCategoryRunner"
        case None => "no source handler registered yet"
      }
    )

  private def newScanSource(opDescClass: Class[_ <: LogicalOp]): ScanSourceOpDesc =
    opDescClass.getDeclaredConstructor().newInstance() match {
      case s: ScanSourceOpDesc => s
      case other =>
        throw new IllegalArgumentException(
          s"${opDescClass.getSimpleName} has no curated handler and is not a " +
            s"ScanSourceOpDesc (${other.getClass.getName})"
        )
    }

  /**
    * Every configuration of one source worth running, as (label, op, its own
    * directory).
    *
    * Each variant gets a directory of its own holding its OWN copy of the fixture,
    * because the generated script reads the file by bare name (`pd.read_csv(
    * "sample.csv")`) out of the directory it runs in. Two variants wanting two
    * different `sample.csv` files cannot share one.
    */
  private def variantsFor(
      opDescClass: Class[_ <: LogicalOp],
      testRoot: Path
  ): Seq[(String, LogicalOp, Path)] = {
    // Punctuation collapses to '_', so two labels differing only in punctuation
    // would name the same directory and share one fixture and one output dir. Fail
    // loudly instead of letting a variant quietly run someone else's file.
    val taken = mutable.Set.empty[String]
    def dirFor(label: String): Path = {
      val name = label.replaceAll("[^A-Za-z0-9]+", "_")
      require(taken.add(name), s"two variants of $opDescClass both map to the directory '$name'")
      Files.createDirectories(testRoot.resolve(name))
    }

    curatedHandlersByClass.get(opDescClass) match {
      case Some(handler) =>
        val baseDir = dirFor("default")
        val base = handler.makeOpDesc(baseDir)
        // Every variant calls the handler AGAIN rather than reusing `base`: the handler
        // writes its fixture into the directory it is given, and a second op carrying
        // the first one's `fileName` would read a file outside the directory it runs in.
        // No enum sweep: both curated sources are the text family, whose `attributeType`
        // says how to PARSE the fixture (`alice` is not an integer) and whose
        // `fileEncoding` describes its BYTES — flipping either without rewriting the
        // fixture compares nothing but how the two paths fail. The auto branch below
        // rewrites its fixture per variant and does sweep them.
        ConfigGenerator
          .fullVariantEditsOf(base, Map.empty, handler.rowCount, sweepEnums = false)
          .fold(
            reason =>
              throw new IllegalStateException(
                s"cannot vary ${opDescClass.getSimpleName}: $reason"
              ),
            identity
          )
          .map { variant =>
            if (variant.at.isEmpty) ("default", base, baseDir)
            else {
              val dir = dirFor(variant.label)
              val op = ConfigGenerator
                .applyVariant(handler.makeOpDesc(dir), variant)
                .fold(
                  reason =>
                    throw new IllegalStateException(
                      s"cannot build ${opDescClass.getSimpleName} variant '${variant.label}': $reason"
                    ),
                  identity
                )
              (variant.label, op, dir)
            }
          } ++ handler.scenarios.map {
          case (label, write) =>
            val dir = dirFor(label)
            (label, write(dir), dir)
        }
      case None =>
        val fileType = declaredFileType(opDescClass).getOrElse("")
        val encoder = encoderByFileType.getOrElse(
          fileType,
          throw new IllegalArgumentException(
            s"No encoder for ${opDescClass.getSimpleName} (fileTypeName='$fileType')"
          )
        )
        def bare(
            label: String,
            rows: Seq[Tuple],
            window: (Option[Int], Option[Int]) = (None, None)
        ): (String, LogicalOp, Path) = {
          val dir = dirFor(label)
          val op = newScanSource(opDescClass)
          op.fileName = Some(encoder(dir, op.fileEncoding.getCharset, rows).toUri.toString)
          op.offset = window._1
          op.limit = window._2
          (label, op, dir)
        }
        // The nulls variant takes the bare config rather than crossing with the
        // others: how a reader treats an empty cell is a property of the reader,
        // and multiplying it across every knob buys runtime, not signal.
        //
        // The windows are the ones the generator never fills, since it fills a
        // knob with a value the property editor accepts: a limit of no rows, which
        // still has columns to declare, a negative bound, which only a plan posted
        // to the API can carry and which pandas reads from the end, and bounds
        // whose sum is past what an Int holds.
        val rows = CanonicalSourceFixture.rows
        Seq(
          bare("default", rows),
          bare("nulls", CanonicalSourceFixture.holedRows),
          bare("window: limit 0", rows, (None, Some(0))),
          bare("window: negative offset", rows, (Some(-1), None)),
          bare("window: negative limit", rows, (None, Some(-1))),
          bare("window: negative", rows, (Some(-1), Some(-1))),
          bare("window: long end", rows, (Some(1), Some(Int.MaxValue))),
          bare("window: largest", rows, (Some(Int.MaxValue), Some(Int.MaxValue)))
        ) ++ skippedBadLine(opDescClass, fileType, encoder, dirFor) ++
          generatedVariants(opDescClass, encoder, dirFor)
    }
  }

  /** A JSONL file whose first line is no JSON at all, read past it. The executor
    * drops and takes the raw lines before it parses any, so a line outside the
    * window is never read, however malformed. The other formats have no line a
    * reader refuses to parse.
    */
  private def skippedBadLine(
      opDescClass: Class[_ <: LogicalOp],
      fileType: String,
      encoder: (Path, Charset, Seq[Tuple]) => Path,
      dirFor: String => Path
  ): Seq[(String, LogicalOp, Path)] =
    if (fileType != "JSONL") Seq.empty
    else {
      val label = "window: skips a bad line"
      val dir = dirFor(label)
      val op = newScanSource(opDescClass)
      val path = encoder(dir, op.fileEncoding.getCharset, CanonicalSourceFixture.rows)
      val body = new String(Files.readAllBytes(path), op.fileEncoding.getCharset)
      Files.write(path, ("not json at all\n" + body).getBytes(op.fileEncoding.getCharset))
      op.fileName = Some(path.toUri.toString)
      op.offset = Some(1)
      Seq((label, op, dir))
    }

  /**
    * The variants the shared [[ConfigGenerator]] derives from the operator's own
    * fields — the base config with every knob filled, plus one per enum branch
    * (`hasHeader`, JSONL's `flatten`). Nothing to register per operator: a knob
    * added to a source is swept the day it is added.
    *
    * `fileEncoding` is swept like any other enum, and the fixture FOLLOWS it: each
    * variant's file is written in the charset that variant declares. Encoding is a
    * statement about the bytes, so a UTF_16 config over a file left in UTF-8 would
    * only compare how each path fails.
    *
    * Variants that serialize identically are dropped — an operator that ignores
    * `fileEncoding` (Arrow declares `@JsonIgnoreProperties`) would otherwise run the
    * same config three times.
    */
  private def generatedVariants(
      opDescClass: Class[_ <: LogicalOp],
      encoder: (Path, Charset, Seq[Tuple]) => Path,
      dirFor: String => Path
  ): Seq[(String, LogicalOp, Path)] = {
    val seen = mutable.Set.empty[String]
    ConfigGenerator
      .generateVariants(opDescClass, Map.empty, CanonicalSourceFixture.rows.size)
      .fold(
        reason =>
          throw new IllegalStateException(
            s"cannot auto-configure ${opDescClass.getSimpleName}: $reason"
          ),
        identity
      )
      .flatMap {
        case (label, op) =>
          val scan = op.asInstanceOf[ScanSourceOpDesc]
          val shape = objectMapper.valueToTree[ObjectNode](scan)
          shape.remove("fileName") // every variant reads its own copy of the file
          if (!seen.add(shape.toString)) None
          else {
            // "default" is already the bare newInstance config above; this one is
            // the generator's, which additionally fills limit and offset.
            val name = if (label == "default") "auto-base" else label
            val dir = dirFor(name)
            scan.fileName = Some(
              encoder(dir, scan.fileEncoding.getCharset, CanonicalSourceFixture.rows).toUri.toString
            )
            Some((name, scan: LogicalOp, dir))
          }
      }
  }

  /** Runs the parity test for the operator, once per variant. Throws on mismatch. */
  def run(opDescClass: Class[_ <: LogicalOp]): Unit = {
    val testRoot = Files.createTempDirectory(s"op-behavior-${opDescClass.getSimpleName}-")
    variantsFor(opDescClass, testRoot).foreach {
      case (label, opDesc, workDir) =>
        try runVariant(opDesc, workDir)
        catch {
          case e: Throwable =>
            throw new AssertionError(s"[variant: $label] ${e.getMessage}", e)
        }
    }
  }

  /** Drive one configured source through both paths inside `workDir`, which holds
    * that variant's fixture, and assert the two tables match.
    */
  private def runVariant(opDesc: LogicalOp, workDir: Path): Unit = {
    val actualDir = workDir.resolve("actual")
    Files.createDirectories(actualDir)

    val pathA = OpExecHarness.execute(opDesc, inputs = Map.empty, outputDir = actualDir)
    val pathB = StandaloneRunner.run(
      opDesc = opDesc,
      inputs = Map.empty,
      outputPortCount = 1,
      workDir = workDir,
      // What the read declared its columns to be, so an integral one is written
      // the way the engine's writer wrote it.
      outputSchemas = pathA.outputSchemas
    )

    val actual = pathA.outputs(PortIdentity(0))
    val expected = pathB.outputs(1)
    Comparator.assertEqual(actual, expected)
    assertDtypesFit(pathA.outputSchemas(PortIdentity(0)), expected)
  }

  /** Whether a pandas dtype is one a Texera column of this type can be left in.
    *
    * The values alone cannot tell a width or a zone apart: a float32 writes the
    * same text as a float64, and the zone is gone once the timestamp is text. Both
    * still reach the next operator, where a float32 sums to another number and a
    * zoned column refuses a cast to a plain datetime. So what the script left
    * each column in is compared with what the read declared, apart from the
    * values. Only the widths and zones Texera has no column for are refused.
    */
  private def fits(attributeType: AttributeType, dtype: String): Boolean =
    attributeType match {
      case AttributeType.INTEGER   => Set("int32", "Int32", "int64", "Int64").contains(dtype)
      case AttributeType.LONG      => Set("int64", "Int64").contains(dtype)
      case AttributeType.DOUBLE    => Set("float64", "Float64").contains(dtype)
      case AttributeType.BOOLEAN   => Set("bool", "boolean").contains(dtype)
      case AttributeType.STRING    => dtype == "object" || dtype == "str" || dtype.startsWith("string")
      case AttributeType.TIMESTAMP => dtype.startsWith("datetime64[") && !dtype.contains(",")
      case _                       => true
    }

  private def assertDtypesFit(declared: Schema, output: Path): Unit = {
    val dtypes =
      objectMapper.readTree(Files.readAllBytes(Path.of(output.toString + ".dtypes.json")))
    // An output with no rows reads back with no columns on either side, so the
    // values cannot say which columns the frame had. What the read declared and
    // what the script left are both still on record.
    val left = dtypes.fieldNames.asScala.toList
    if (left != declared.getAttributeNames)
      throw new AssertionError(
        s"the script left the columns $left where the read declared ${declared.getAttributeNames}"
      )
    val misfits = declared.getAttributes.flatMap { attribute =>
      Option(dtypes.get(attribute.getName))
        .map(_.asText)
        .filterNot(fits(attribute.getType, _))
        .map(dtype => s"${attribute.getName}: declared ${attribute.getType}, left as $dtype")
    }
    if (misfits.nonEmpty)
      throw new AssertionError(
        s"the script left columns in a dtype the read did not declare:\n  ${misfits.mkString("\n  ")}"
      )
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

  /** How many rows the fixture holds. Only the handler knows — it writes its own,
    * rather than the shared [[CanonicalSourceFixture]]. A row-window knob the
    * variants fill (`limit`, `offset`) is sized against this, so that the value they
    * take keeps some rows and drops some instead of landing past the end.
    */
  def rowCount: Int

  /** Configurations the sweep cannot reach, each with the file it has to read:
    * a type the default text cannot be parsed as, or bytes in another charset.
    * Each one writes into the directory it is handed and returns the configured
    * op. Default: none.
    */
  def scenarios: Seq[(String, Path => LogicalOp)] = Seq.empty
}

/**
  * The rows every structured-file source reads: [[CanonicalFixture]]'s, whole.
  *
  * A source has no input port, so the fixture is delivered not as an input JSONL
  * but as a file the operator opens itself. Each `writeXxx` encodes these rows
  * into one on-disk format (CSV / JSONL / Arrow); a source handler picks the
  * encoder its operator understands and points `fileName` at the result. So CSV,
  * CSVOld, JSONL and Arrow all verify that the operator reconstructs one shared
  * table, instead of each asserting against its own ad-hoc sample.
  *
  * It reads the canonical table rather than a narrow one of its own. A source
  * fixture picked for the types that survive a round trip would be choosing not
  * to ask the question this suite exists to ask: these files carry no types, both
  * readers infer, and where they infer differently is exactly what should show. A
  * date column does part them, and [[StandaloneRunner.sourceCasts]] is where that
  * is settled — on Path B's reading, not by leaving the column out.
  */
object CanonicalSourceFixture {

  /** Columns only a reader has a question about, appended to the canonical ones.
    * They stay out of [[CanonicalFixture]], which every transform reads.
    *
    * `a"b\c_big` holds odd integers past 2^53, none of which a float can hold, so
    * a reader that widens the column through one rounds every value. `a"b\c_region`
    * holds the text `NA`, which pandas reads as missing unless told not to.
    */
  private val sourceColumns: Seq[(Attribute, Int => AnyRef)] = Seq(
    new Attribute("a\"b\\c_big", AttributeType.LONG) -> (i => Long.box(9007199254740993L + 2 * i)),
    new Attribute("a\"b\\c_region", AttributeType.STRING) -> (i => Seq("NA", "EU", "APAC")(i % 3))
  )

  val schema: Schema =
    new Schema(CanonicalFixture.schema.getAttributes ++ sourceColumns.map(_._1): _*)

  val rows: Vector[Tuple] = CanonicalFixture.allRows.zipWithIndex.map {
    case (canonical, i) =>
      val b = Tuple.builder(schema)
      CanonicalFixture.schema.getAttributes.foreach { a =>
        b.add(a, canonical.getField[AnyRef](a.getName))
      }
      sourceColumns.foreach { case (a, value) => b.add(a, value(i)) }
      b.build()
  }

  /** [[rows]] with one empty cell per column, for the `nulls` variant. Nothing is
    * kept filled: a source reads every column the same way and pairs no rows, so
    * no column's value is load-bearing.
    */
  val holedRows: Seq[Tuple] = SharedFixture.emptyOneCellPerColumn(rows, schema, Set.empty)

  /** Write the rows as a header-first, comma-delimited CSV encoded in `charset`.
    *
    * The charset is a parameter because it describes the BYTES, not the config: a
    * variant declaring `fileEncoding = UTF_16` over a file left in UTF-8 would
    * compare nothing but how each path fails.
    *
    * Two headers are left blank, a timestamp's and a long's, which the reader
    * names column-N and pandas names "Unnamed: N" until it is told otherwise, so
    * a type asked for by the schema's name finds no column. The header at
    * position 1 is written as the text "Unnamed: 1", a name a user can really
    * choose and pandas' placeholder for the same place.
    */
  def writeCsv(dir: Path, charset: Charset, rows: Seq[Tuple]): Path = {
    val path = dir.resolve("sample.csv")
    val blank = Set("start_ts", "a\"b\\c_big")
    val header = schema.getAttributes.zipWithIndex
      .map {
        case (_, 1)                              => "Unnamed: 1"
        case (a, _) if blank.contains(a.getName) => ""
        case (a, _)                              => csvField(a.getName)
      }
      .mkString(",")
    val body = rows.map { t =>
      schema.getAttributes
        .map(a => csvField(Option(t.getField[AnyRef](a.getName)).map(_.toString).orNull))
        .mkString(",")
    }
    Files.write(path, ((header +: body).mkString("\n") + "\n").getBytes(charset))
    path
  }

  /** One CSV field, quoted per RFC 4180.
    *
    * The table carries commas inside values — a bracketed edge pair, a
    * comma-delimited list, an ordinary English sentence — and writing those raw
    * shifts every column after them. What the two paths then disagree about is a
    * broken file rather than anything either of them does.
    */
  private def csvField(value: String): String =
    if (value == null) ""
    else if (value.exists(c => c == ',' || c == '"' || c == '\n' || c == '\r'))
      "\"" + value.replace("\"", "\"\"") + "\""
    else value

  /** Write the rows as JSON Lines (one object per line, keys in schema order).
    * Reuses [[TupleIO.writeTuples]] — the same writer the transform fixtures
    * use; it also drops a `.schema.json` sidecar the source ignores.
    *
    * That writer is shared and always writes UTF-8, so a variant asking for another
    * charset gets the bytes transcoded afterwards rather than a second writer.
    *
    * A hole is written two ways, because the executor reads them differently: a
    * key left out is a null, and a key holding null is the text "null". A column
    * at an even position keeps the key, one at an odd position leaves it out.
    *
    * Each record also nests, the one thing JSON holds that a table does not: an
    * array of objects, an array of text whose length differs between records, and
    * an object holding a date written month first. Flattening names every element
    * by its position, and the date is only a date once it is parsed.
    */
  def writeJsonl(dir: Path, charset: Charset, rows: Seq[Tuple]): Path = {
    val path = dir.resolve("sample.jsonl")
    TupleIO.writeTuples(path, rows.iterator, schema)
    val leftOut = schema.getAttributes.zipWithIndex.collect {
      case (attribute, i) if i % 2 == 1 => attribute.getName
    }
    val lines = new String(Files.readAllBytes(path), StandardCharsets.UTF_8)
      .split("\n")
      .filter(_.nonEmpty)
      .zipWithIndex
      .map {
        case (line, i) =>
          val node = objectMapper.readTree(line).asInstanceOf[ObjectNode]
          leftOut.filter(name => node.has(name) && node.get(name).isNull).foreach(node.remove)
          addNested(node, i)
          objectMapper.writeValueAsString(node)
      }
    Files.write(path, lines.map(_ + "\n").mkString.getBytes(charset))
    path
  }

  /** The nested values of record `i`. The first item's `id` is a long past 2^53,
    * missing from record 1, so the flattened `items1.id` is a nullable long.
    */
  private def addNested(node: ObjectNode, i: Int): Unit = {
    val items = node.putArray("a\"b\\c_items")
    val first = items.addObject()
    if (i != 1) first.put("id", 9007199254740993L + 2 * i)
    items.addObject().put("id", i)
    val tags = node.putArray("a\"b\\c_tags")
    tags.add("x")
    if (i % 2 == 0) tags.add("y")
    node.putObject("a\"b\\c_meta").put("when", f"${i % 12 + 1}%02d/15/${2024 + i % 2} 10:00:00")
  }

  /** Write the rows as an uncompressed Arrow IPC ("file" format) stream — the
    * format both `ArrowFileReader` (Path A) and `pd.read_feather` (Path B)
    * read.
    */
  def writeArrow(dir: Path, rows: Seq[Tuple]): Path = {
    val path = dir.resolve("sample.arrow")
    // Texera's own Schema-to-Arrow mapping and tuple writer, so the file carries
    // exactly the types `ArrowUtils.toTexeraSchema` reads back on the other side.
    // Hand-listing the fields is what let the table outgrow them unnoticed: the
    // columns past the list were simply not written, and both paths went on
    // agreeing about the few that were.
    val arrowSchema = ArrowUtils.fromTexeraSchema(schema)
    Using.Manager { use =>
      val allocator = use(new RootAllocator())
      val texera = use(VectorSchemaRoot.create(arrowSchema, allocator))
      texera.allocateNew()
      rows.zipWithIndex.foreach { case (t, i) => ArrowUtils.setTexeraTuple(t, i, texera) }
      val extras = arrowOnlyColumns(allocator, rows.size).map(use(_))
      val vectors = texera.getFieldVectors.asScala.toSeq ++ extras
      val withNote =
        new ArrowSchema(vectors.map(_.getField).asJava, Map("pandas" -> PandasIndexNote).asJava)
      val root = use(new VectorSchemaRoot(withNote, vectors.asJava, rows.size))
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

  /** The note pandas leaves in the schema of a file it wrote from a frame keyed
    * by `id`. pandas reads it back and makes `id` the index rather than a column;
    * the executor reads the columns the file states and knows of no index.
    */
  private val PandasIndexNote: String =
    """{"index_columns": ["id"], "column_indexes": [], "columns": [{"name": "id", """ +
      """"field_name": "id", "pandas_type": "int32", "numpy_type": "int32", "metadata": null}], """ +
      """"pandas_version": "2.2.3"}"""

  /** Columns in the widths and zones an Arrow file states and Texera's own
    * mapping never writes, since it writes every float as a double and every
    * integer as a signed 32 or 64 bits. Real files carry them: pyarrow writes a
    * numpy float32, int16 or uint32 array as one.
    *
    * Row 0 holds the value each width cannot be read out of by accident: 2^24 in
    * single precision, a negative in eight bits, and the largest value each
    * unsigned width counts to, which its storage holds as -1. Row 1 is null in
    * every one. The timestamp names a zone other than UTC, so the wall clock read
    * is the file's.
    *
    * The values reach the executor's reading. The width and zone the script
    * leaves each column in write out as the same values, and are what
    * [[SourceCategoryRunner.assertDtypesFit]] checks.
    */
  private def arrowOnlyColumns(allocator: RootAllocator, rowCount: Int): Seq[FieldVector] = {
    val f32 = new Float4Vector("a\"b\\c_f32", allocator)
    val i16 = new SmallIntVector("a\"b\\c_i16", allocator)
    val i8 = new TinyIntVector("a\"b\\c_i8", allocator)
    val u8 = new UInt1Vector("a\"b\\c_u8", allocator)
    val u16 = new UInt2Vector("a\"b\\c_u16", allocator)
    val u32 = new UInt4Vector("a\"b\\c_u32", allocator)
    val zoned = new TimeStampMilliTZVector("a\"b\\c_zoned", allocator, "Asia/Tokyo")
    val all = Seq[FieldVector](f32, i16, i8, u8, u16, u32, zoned)
    all.foreach(_.allocateNew())
    (0 until rowCount).foreach { i =>
      if (i == 1) all.foreach(_.setNull(i))
      else {
        f32.setSafe(i, if (i == 0) 16777216.0f else i.toFloat)
        i16.setSafe(i, (i * 7).toShort)
        i8.setSafe(i, (if (i == 0) -3 else i).toByte)
        u8.setSafe(i, if (i == 0) 0xff else i)
        u16.setSafe(i, if (i == 0) 0xffff else i)
        u32.setSafe(i, if (i == 0) -1 else i)
        zoned.setSafe(i, i * 3600000L)
      }
    }
    all.foreach(_.setValueCount(rowCount))
    all
  }

  /** Write the rows as a Parquet file carrying the canonical table's own types.
    *
    * The mapping below is the inverse of `ParquetSchemaMapping`, which is what
    * the operator reads the file back with. Writing every column as optional is
    * what lets a hole be a hole: Parquet has no null value, only a field that
    * repeats zero times, so a required column could not hold one.
    */
  def writeParquet(dir: Path, rows: Seq[Tuple]): Path = {
    val path = dir.resolve("sample.parquet")
    val messageType = parquetSchemaOf(schema)
    val conf = new Configuration()
    GroupWriteSupport.setSchema(messageType, conf)
    val factory = new SimpleGroupFactory(messageType)
    Using(
      ExampleParquetWriter
        .builder(new HadoopPath(path.toString))
        .withConf(conf)
        .withType(messageType)
        .build()
    ) { writer => rows.foreach(t => writer.write(groupOf(factory, t))) }.get
    path
  }

  /** The canonical schema as Parquet states types. */
  private def parquetSchemaOf(schema: Schema): MessageType = {
    val fields = schema.getAttributes.map { attribute =>
      val name = attribute.getName
      attribute.getType match {
        case AttributeType.STRING =>
          Types
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named(name)
        case AttributeType.INTEGER => Types.optional(PrimitiveTypeName.INT32).named(name)
        case AttributeType.LONG    => Types.optional(PrimitiveTypeName.INT64).named(name)
        case AttributeType.DOUBLE  => Types.optional(PrimitiveTypeName.DOUBLE).named(name)
        case AttributeType.BOOLEAN => Types.optional(PrimitiveTypeName.BOOLEAN).named(name)
        case AttributeType.TIMESTAMP =>
          Types
            .optional(PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named(name)
        case other =>
          throw new UnsupportedOperationException(s"no Parquet type for $other in column $name")
      }
    }
    Types.buildMessage().addFields(fields.toSeq: _*).named("canonical")
  }

  /** One row, with an absent field wherever the tuple holds a null. */
  private def groupOf(factory: SimpleGroupFactory, tuple: Tuple): Group = {
    val group = factory.newGroup()
    schema.getAttributes.foreach { attribute =>
      val name = attribute.getName
      Option(tuple.getField[AnyRef](name)).foreach {
        case v: java.lang.String  => group.append(name, v)
        case v: java.lang.Integer => group.append(name, v.intValue())
        case v: java.lang.Long    => group.append(name, v.longValue())
        case v: java.lang.Double  => group.append(name, v.doubleValue())
        case v: java.lang.Boolean => group.append(name, v.booleanValue())
        // The wall clock counted from the epoch with no zone in the picture,
        // which is what `ArrowUtils` means by a Texera TIMESTAMP and what the
        // operator reads back. `getTime` would bake in the writer's own zone.
        case v: java.sql.Timestamp =>
          group.append(name, v.toLocalDateTime.toInstant(ZoneOffset.UTC).toEpochMilli)
        case other =>
          throw new UnsupportedOperationException(
            s"no Parquet value for ${other.getClass.getSimpleName} in column $name"
          )
      }
    }
    group
  }
}

/** Handler for `TextInputSourceOpDesc`. The text lives in the config — no fixture file. */
object TextInputHandler extends SourceHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[TextInputSourceOpDesc]

  override val rowCount: Int = 3

  override def makeOpDesc(testRoot: Path): LogicalOp = {
    val desc = new TextInputSourceOpDesc()
    desc.textInput = "alice\nbob\ncarol"
    desc // defaults: attributeType STRING (one row per line), attributeName "line"
  }

  private def typed(
      text: String,
      attributeType: FileAttributeType,
      offset: Option[Int]
  ): LogicalOp = {
    val desc = new TextInputSourceOpDesc()
    desc.textInput = text
    desc.attributeType = attributeType
    desc.fileScanOffset = offset
    desc
  }

  /** A line the window skips is never parsed, so it may be one the type refuses.
    * A boolean line is read as the engine reads one, where 1 is true.
    */
  override def scenarios: Seq[(String, Path => LogicalOp)] =
    Seq(
      "integers past a line skipped" -> (_ =>
        typed("not a number\n1\n2\n3", FileAttributeType.INTEGER, Some(1))
      ),
      "booleans" -> (_ => typed("true\nFALSE\n1\n0", FileAttributeType.BOOLEAN, None))
    )
}

/** Handler for `FileScanSourceOpDesc`. Plain text file read in default line mode. */
object FileScanSourceHandler extends SourceHandler {

  override val opDescClass: Class[_ <: LogicalOp] = classOf[FileScanSourceOpDesc]

  override val rowCount: Int = 3

  override def makeOpDesc(testRoot: Path): LogicalOp = {
    val txtPath = testRoot.resolve("sample.txt")
    Files.write(txtPath, "alice\nbob\ncarol\n".getBytes(StandardCharsets.UTF_8))

    val desc = new FileScanSourceOpDesc()
    desc.fileName = Some(txtPath.toUri.toString)
    desc // defaults: attributeType STRING (one row per line), attributeName "line"
  }

  /** `extract`, `outputFileName` and `encoding` are vals, so they are
    * deserialized in rather than set.
    */
  private def described(
      file: Path,
      attributeType: FileAttributeType,
      fields: String*
  ): LogicalOp = {
    val desc = objectMapper.readValue(
      (""""operatorType":"FileScan"""" +: fields).mkString("{", ",", "}"),
      classOf[FileScanSourceOpDesc]
    )
    desc.fileName = Some(file.toUri.toString)
    desc.attributeType = attributeType
    desc
  }

  private def text(dir: Path, content: String, charset: Charset = StandardCharsets.UTF_8): Path =
    Files.write(dir.resolve("sample.txt"), content.getBytes(charset))

  /** Two entries and the one macOS adds beside them, which the engine skips. */
  private def archive(dir: Path): Path = {
    val path = dir.resolve("sample.zip")
    Using(new ZipOutputStream(Files.newOutputStream(path))) { out =>
      Seq("a.txt" -> "line1\nline2\n", "b.txt" -> "line3\n", "__MACOSX/a.txt" -> "junk\n").foreach {
        case (name, content) =>
          out.putNextEntry(new ZipEntry(name))
          out.write(content.getBytes(StandardCharsets.UTF_8))
          out.closeEntry()
      }
    }.get
    path
  }

  /** A line the window skips is never parsed, so it may be one the type refuses.
    * A boolean line is read as the engine reads one, where 1 is true. The charset
    * the Encoding field names is the one the bytes are in. With Extract on, the
    * engine reads the entries inside the archive and names each one by its own
    * name, line by line or whole.
    */
  override def scenarios: Seq[(String, Path => LogicalOp)] =
    Seq(
      "integers past a line skipped" -> (dir =>
        described(
          text(dir, "not a number\n1\n2\n3\n"),
          FileAttributeType.INTEGER,
          """"fileScanOffset":1"""
        )
      ),
      "booleans" -> (dir => described(text(dir, "true\nFALSE\n1\n0\n"), FileAttributeType.BOOLEAN)),
      "UTF-16" -> (dir =>
        described(
          text(dir, "première\ndeuxième\n", StandardCharsets.UTF_16),
          FileAttributeType.STRING,
          """"encoding":"UTF_16""""
        )
      ),
      "archive" -> (dir => described(archive(dir), FileAttributeType.STRING, """"extract":true""")),
      "archive, lines named" -> (dir =>
        described(
          archive(dir),
          FileAttributeType.STRING,
          """"extract":true""",
          """"outputFileName":true"""
        )
      ),
      "archive, entries named" -> (dir =>
        described(
          archive(dir),
          FileAttributeType.SINGLE_STRING,
          """"extract":true""",
          """"outputFileName":true"""
        )
      )
    )
}
