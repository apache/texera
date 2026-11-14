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

import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.operator.source.fetcher.URLFetcherOpDesc
import org.apache.texera.amber.operator.source.scan.{FileAttributeType, ScanSourceOpDesc}
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.csvOld.CSVOldScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.file.FileScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Try

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
    * isn't the shared table (text-family, single `line` column) or
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
    *
    * An encoder is handed the op it writes for, because a source's config states
    * how the bytes look as well as how to read them: a delimiter, like an
    * encoding, has to be in the file before a reader can be asked to honour it.
    */
  private val encoderByFileType: Map[String, (Path, ScanSourceOpDesc, Seq[Tuple]) => Path] = Map(
    "CSV" -> ((dir, op, rows) =>
      CanonicalSourceFixture.writeCsv(dir, op.fileEncoding.getCharset, delimiterOf(op), rows)
    ),
    "CSVOld" -> ((dir, op, rows) =>
      CanonicalSourceFixture.writeCsv(dir, op.fileEncoding.getCharset, delimiterOf(op), rows)
    ),
    "JSONL" -> ((dir, op, rows) =>
      CanonicalSourceFixture.writeJsonl(dir, op.fileEncoding.getCharset, rows)
    ),
    // Arrow is binary and its descriptor declares fileEncoding ignored, so the
    // charset a variant asks for has nothing to apply to.
    "Arrow" -> ((dir, _, rows) => CanonicalSourceFixture.writeArrow(dir, rows)),
    "Parquet" -> ((dir, _, rows) => CanonicalSourceFixture.writeParquet(dir, rows))
  )

  /** The delimiter a CSV variant reads with, resolved as the readers resolve it:
    * the first character, and a comma when the field is empty or unset.
    */
  private def delimiterOf(op: ScanSourceOpDesc): Char =
    (op match {
      case csv: CSVScanSourceOpDesc       => csv.customDelimiter
      case csvOld: CSVOldScanSourceOpDesc => csvOld.customDelimiter
      case _                              => None
    }).filter(_.nonEmpty).map(_.charAt(0)).getOrElse(',')

  /**
    * Sources this runner cannot verify, with the honest reason. Mirrors
    * `TransformVerificationRunner.knownIssues`: the reason surfaces in the
    * ignored test's name and the coverage table.
    */
  private val knownIssues: Map[Class[_ <: LogicalOp], String] = Map(
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

  /** Whether this runner is the one to verify `opDescClass`: a source with no
    * input port. A source that reads its file names from an input port, as File
    * Scan does, is fed a table like a transform instead.
    */
  def runsAsSource(opDescClass: Class[_ <: LogicalOp]): Boolean =
    classOf[SourceOperatorDescriptor].isAssignableFrom(opDescClass) &&
      Try(opDescClass.getDeclaredConstructor().newInstance().operatorInfo.inputPorts.isEmpty)
        .getOrElse(true)

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
          op.fileName = Some(encoder(dir, op, rows).toUri.toString)
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
      encoder: (Path, ScanSourceOpDesc, Seq[Tuple]) => Path,
      dirFor: String => Path
  ): Seq[(String, LogicalOp, Path)] =
    if (fileType != "JSONL") Seq.empty
    else {
      val label = "window: skips a bad line"
      val dir = dirFor(label)
      val op = newScanSource(opDescClass)
      val path = encoder(dir, op, CanonicalSourceFixture.rows)
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
    * only compare how each path fails. A CSV delimiter is swept the same way and
    * followed the same way: a semicolon config over a comma file reads each line as
    * one field on both paths, and agreeing on that tests no semicolon at all.
    *
    * Variants that serialize identically are dropped — an operator that ignores
    * `fileEncoding` (Arrow declares `@JsonIgnoreProperties`) would otherwise run the
    * same config three times.
    */
  private def generatedVariants(
      opDescClass: Class[_ <: LogicalOp],
      encoder: (Path, ScanSourceOpDesc, Seq[Tuple]) => Path,
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
            scan.fileName = Some(encoder(dir, scan, CanonicalSourceFixture.rows).toUri.toString)
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
    * a type the default text cannot be parsed as, bytes in another charset, or
    * an archive to extract.
    * Each one writes into the directory it is handed and returns the configured
    * op. Default: none.
    */
  def scenarios: Seq[(String, Path => LogicalOp)] = Seq.empty
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
    * A boolean line is read as the engine reads one, where 1 is true. The
    * timestamp lines are the ones File Scan's files carry.
    */
  override def scenarios: Seq[(String, Path => LogicalOp)] =
    Seq(
      "integers past a line skipped" -> (_ =>
        typed("not a number\n1\n2\n3", FileAttributeType.INTEGER, Some(1))
      ),
      "booleans" -> (_ => typed("true\nFALSE\n1\n0", FileAttributeType.BOOLEAN, None)),
      "timestamps" -> (_ =>
        typed(FileScanTransformHandler.TimestampLines, FileAttributeType.TIMESTAMP, None)
      )
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

  /** File Scan's files, read from this operator's own path field. The two
    * operators share the engine's reader and differ only in where the path
    * comes from.
    */
  override def scenarios: Seq[(String, Path => LogicalOp)] =
    FileScanTransformHandler.files.map {
      case (label, write) =>
        label -> { (dir: Path) =>
          val (file, attributeType, fields) = write(dir)
          described(file, attributeType, fields: _*)
        }
    }
}
