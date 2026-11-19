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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HadoopPath}
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.{ExampleParquetWriter, GroupWriteSupport}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{LogicalTypeAnnotation, MessageType, Types}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.nio.channels.FileChannel
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, Path, StandardOpenOption}
import java.sql.Timestamp
import java.time.ZoneOffset
import scala.jdk.CollectionConverters._
import scala.util.Using

/**
  * The shared input dataset for auto-configured transform verification.
  * Properties are deliberate (see CanonicalFixtureSpec): enough rows and
  * partial port-0/port-1 overlap to defeat hash-coincidence false passes on
  * set ops and joins, and the canonical value "1" present in some-but-not-all
  * rows so ConfigGenerator-filled free-form predicates match a proper subset.
  */
object CanonicalFixture extends SharedFixture {

  // Columns are semantically named and type-correct so @SampleColumn-tagged or
  // type-constrained fields can be filled with realistic input (a valid OHLC
  // block, real ISO country codes, real dates) instead of a degenerate
  // first-column pick. Ordering is deliberate: id/name/score lead so the
  // first-column fallback AND the type-rule tier ("first column of a matching
  // type") are unchanged for un-annotated fields — the domain-specific columns
  // that follow are only reached via an explicit @SampleColumn.
  val schema: Schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING),
    new Attribute("score", AttributeType.DOUBLE),
    new Attribute("open", AttributeType.DOUBLE),
    new Attribute("high", AttributeType.DOUBLE),
    new Attribute("low", AttributeType.DOUBLE),
    new Attribute("close", AttributeType.DOUBLE),
    new Attribute("iso_country", AttributeType.STRING),
    new Attribute("trade_date", AttributeType.STRING),
    // --- Domain-specific columns (reached only via @SampleColumn) ---
    new Attribute("pvalue", AttributeType.DOUBLE), // strictly in (0,1): p-values
    new Attribute("log2fc", AttributeType.DOUBLE), // signed, centered on 0: fold-change
    new Attribute("comp_a", AttributeType.DOUBLE), // >0 ternary simplex component
    new Attribute("comp_b", AttributeType.DOUBLE), // >0 ternary simplex component
    new Attribute("comp_c", AttributeType.DOUBLE), // >0 ternary simplex component
    new Attribute("uvec", AttributeType.DOUBLE), // any real: a 4th numeric (Quiver u/v)
    new Attribute(
      "edge_pair",
      AttributeType.STRING
    ), // "[parent, child]" literals, single-rooted tree
    new Attribute("node_src", AttributeType.STRING), // edge source id (Sankey/Network)
    new Attribute("node_dst", AttributeType.STRING), // edge target id, overlaps node_src (a DAG)
    new Attribute(
      "start_ts",
      AttributeType.TIMESTAMP
    ), // real timestamp; Gantt start / TimeSeries axis
    new Attribute(
      "a\"b\\c_finish_ts",
      AttributeType.TIMESTAMP
    ), // always > start_ts; Gantt finish (bar width)
    new Attribute(
      "a\"b'c\\d\ne_uniq_name",
      AttributeType.STRING
    ), // distinct per row: Pie/name-keyed ops need no duplicates
    new Attribute(
      "a\"b\\c_simplex_a",
      AttributeType.DOUBLE
    ), // >0, and the three sum to 100 in every row (ternary-contour)
    new Attribute("a\"b\\c_simplex_b", AttributeType.DOUBLE), // >0 simplex component summing to 100
    new Attribute("a\"b\\c_simplex_c", AttributeType.DOUBLE), // >0 simplex component summing to 100
    // ── text + iris-numeric columns for Hugging Face model operators ──
    new Attribute(
      "short_text",
      AttributeType.STRING
    ), // one sentence: sentiment / spam-detection input
    new Attribute(
      "long_text",
      AttributeType.STRING
    ), // a multi-sentence paragraph: summarization input
    new Attribute("petal_length", AttributeType.DOUBLE), // iris petal length in cm (~1.3–6.5)
    new Attribute("petal_width", AttributeType.DOUBLE), // iris petal width in cm (~0.2–2.4)
    new Attribute(
      "a\"b\\c_species",
      AttributeType.INTEGER
    ), // the 0/1 iris class, exactly `petal_length >= 3.9`: the label the sklearn
    // families fit against, separable by the two petal columns above
    new Attribute(
      "csv_list",
      AttributeType.STRING
    ), // comma-delimited, 1–4 tokens per row: split/explode ops need real fan-out
    new Attribute(
      "mixed_case",
      AttributeType.STRING
    ), // a third lower-case, a third upper, a third letterless: a case flag has to
    // change WHICH rows match, and on any other column it changes nothing
    new Attribute(
      "a\"b\\c_high_score",
      AttributeType.BOOLEAN
    ), // `score >= 3.0`, the table's only boolean. Its TEXT is what no other column
    // can ask: the engine writes "true" where Python's str() writes "True". Six
    // false and nine true, both present in either port window
    new Attribute(
      "a\"b\\c_species_pred",
      AttributeType.INTEGER
    ), // a predictor's guess at the label: the same 0/1 domain, wrong on a few rows.
    // Scoring compares a PAIR of columns, and no single label column supplies one.
    // Last so the first-unused fallback reaches it only after every other column
    new Attribute("species_name", AttributeType.STRING), // the label spelled out
    new Attribute(
      "species_name_pred",
      AttributeType.STRING
    ), // the prediction spelled out. A scorer takes a string label as readily as a
    // numeric one and names the class after it rather than after its position, so
    // the pair exists a second time in text
    // The values a column of each type can hold that its ordinary rows never
    // reach, last so no column-picking knob comes to them before the others.
    new Attribute("edge_double", AttributeType.DOUBLE), // 1.0E20 and 1.0E-4 beside
    // ordinary numbers: Java writes a double past 1e7 or under 1e-3 in E notation
    new Attribute("edge_ts", AttributeType.TIMESTAMP), // years 1600, 2500 and 9999,
    // and 1677 and 2262 at pandas' own edges: a Timestamp holds every one of them
    new Attribute("digits", AttributeType.STRING), // text made only of digits, "001"
    // among them, which a reader left to infer takes for a number
    new Attribute("views", AttributeType.LONG) // the one LONG column: counts past
    // 2^31, an int's limit, on both sides of 2147483648, and two that fit an int
  )

  // ── Data source ──
  // The rows are NOT generated at runtime — they live in a single, checked-in,
  // human-readable JSON file that IS the source of truth:
  //   src/test/resources/verify/canonical_fixture.json   (15 rows, ids 1..15)
  // Open it to see the exact table; edit it to change the data. The
  // CanonicalFixtureSpec invariants guard every semantic constraint (valid OHLC
  // block, pvalue ∈ (0,1), ternary parts summing to 100, each finish after its start,
  // etc.), so a hand-edit that breaks one fails the build. `schema` above stays
  // authoritative for column types: JSON has no TIMESTAMP, so the timestamps
  // are stored as JDBC strings ("2024-01-01 00:00:00.0") and coerced back here.
  private val fixtureResource = "/verify/canonical_fixture.json"

  override val allRows: Vector[Tuple] = {
    val stream = Option(getClass.getResourceAsStream(fixtureResource))
      .getOrElse(sys.error(s"canonical fixture not found on classpath: $fixtureResource"))
    val root =
      try new ObjectMapper().readTree(stream)
      finally stream.close()
    root
      .elements()
      .asScala
      .map { node =>
        val b = Tuple.builder(schema)
        schema.getAttributes.foreach { attr =>
          val cell = node.get(attr.getName)
          require(cell != null, s"fixture row missing column '${attr.getName}'")
          val value: AnyRef = attr.getType match {
            case AttributeType.INTEGER   => Int.box(cell.asInt())
            case AttributeType.LONG      => Long.box(cell.asLong())
            case AttributeType.DOUBLE    => Double.box(cell.asDouble())
            case AttributeType.BOOLEAN   => Boolean.box(cell.asBoolean())
            case AttributeType.TIMESTAMP => Timestamp.valueOf(cell.asText())
            case _                       => cell.asText() // STRING
          }
          b.add(attr, value)
        }
        b.build()
      }
      .toVector
  }

  // Each port takes two thirds of the table, from opposite ends, so the ports
  // overlap by the remaining third and no row sits outside both. The overlap is
  // what stops joins and set ops passing by hash coincidence: ports holding the
  // same rows make intersect and union the same answer, and disjoint ports make
  // both empty, which a broken operator produces too. Two thirds rather than a
  // fixed count so a row added to the JSON widens the windows instead of falling
  // off the end, and a port stays well under the whole table — these windows are
  // what every per-test Python run is sized by. At 15 rows this is positions 0-9
  // and 5-14. Rows sit out of id order in the file, so the windows are
  // positional — not id ranges.
  private def windowSize: Int = allRows.size * 2 / 3
  def port0Rows: Seq[Tuple] = allRows.take(windowSize)
  def port1Rows: Seq[Tuple] = allRows.takeRight(windowSize)

  override def rowsFor(port: Int): Seq[Tuple] = if (port == 0) port0Rows else port1Rows

  /** `id` keeps every value: it is what joins and set operations match on, and
    * emptying it would change which rows pair up rather than what a null does.
    */
  override val keepFilled: Set[String] = Set("id")

  /** This table as the sklearn families read it: the two petal columns and the
    * species label, and nothing else, because `X = table.drop(target, axis=1)`
    * hands `fit` every column that is not the target. The two features separate
    * the classes exactly, so an estimator fits them without a tie to break.
    */
  val sklearnNumeric: SharedFixture = ProjectedFixture(
    this,
    Seq("petal_length", "petal_width", "a\"b\\c_species"),
    keepFilled = Set("a\"b\\c_species")
  )

  /** This table minus `score`. An operator whose output column is named `score`
    * by default cannot run here otherwise: it would create a column the input
    * already holds, and the schema refuses the duplicate before the operator
    * runs. Dropping the one column puts the DEFAULT config under test, which is
    * the config a user gets, rather than a hand-written name chosen to dodge the
    * clash.
    */
  val withoutScore: SharedFixture = ProjectedFixture(
    this,
    schema.getAttributeNames.filterNot(_ == "score"),
    keepFilled = keepFilled
  )

  /** This table as a scorer reads it when the labels are text: the same pair as
    * the numeric label and its prediction, spelled out, and nothing else. The scenario
    * that takes it names the two columns itself, since the operator's `@SampleColumn`s
    * name the numeric pair this projection does not carry.
    */
  val scorerTextLabels: SharedFixture = ProjectedFixture(
    this,
    Seq("species_name", "species_name_pred"),
    keepFilled = Set.empty
  )

  /** [[sklearnNumeric]] plus a column an estimator cannot fit, so the narrowing to
    * the fittable columns has something to do. The two paths narrow in different
    * places, the operator once ahead of the port branch and the standalone script
    * once per port, so each has to drop it on its own. The text carries no signal
    * about the label, so the fit is the one the two petal columns already give.
    */
  val sklearnNumericWithText: SharedFixture = ProjectedFixture(
    this,
    Seq("petal_length", "petal_width", "short_text", "a\"b\\c_species"),
    keepFilled = Set("a\"b\\c_species")
  )

  /** This table as the `countVectorizer=true` path reads it: one text column and
    * the same label. `short_text` leads because the model probe feeds a text
    * pipeline the frame's first column as a Series. Every row carrying a given
    * sentence carries the same label (an invariant of the table), so the
    * vectorized classes separate exactly, as the numeric pair does.
    */
  val sklearnText: SharedFixture = ProjectedFixture(
    this,
    Seq("short_text", "long_text", "a\"b\\c_species"),
    keepFilled = Set("a\"b\\c_species")
  )
}

/**
  * The rows every structured-file source reads: [[CanonicalFixture]]'s, whole.
  *
  * A source has no input port, so the fixture is delivered not as an input JSONL
  * but as a file the operator opens itself. Each `writeXxx` encodes these rows
  * into one on-disk format (CSV / JSONL / Arrow / Parquet); [[encoderByFileType]]
  * picks the one the operator declares and points `fileName` at the result. So
  * CSV, CSVOld, JSONL, Arrow and Parquet all verify that the operator reconstructs
  * one shared table, instead of each asserting against its own ad-hoc sample.
  *
  * It reads the canonical table rather than a narrow one of its own. A source
  * fixture picked for the types that survive a round trip would be choosing not
  * to ask the question this suite exists to ask: these files carry no types, both
  * readers infer, and where they infer differently is exactly what should show. A
  * date column does part them, and the exported reader settles that by parsing it
  * as the engine does, not by leaving the column out.
  */
object CanonicalSourceFixture {

  /** Columns only a reader has a question about, appended to the canonical ones.
    * They stay out of [[CanonicalFixture]], which every transform reads.
    *
    * `a"b\c_big` holds odd integers past 2^53, none of which a float can hold, so
    * a reader that widens the column through one rounds every value. `a"b\c_region`
    * holds the text `NA`, which pandas reads as missing unless told not to.
    * `a"b\c_nanos` holds digits past the millisecond, which a reader keeps or cuts
    * as the engine's own does. It stays out of the canonical table, where a
    * Python operator is handed a datetime, which counts no finer than microseconds.
    * `in1df` is named like the variable a script binds an input to, so a plan
    * that reads this file shows whether the translator renames the column too.
    */
  private val sourceColumns: Seq[(Attribute, Int => AnyRef)] = Seq(
    new Attribute("a\"b\\c_big", AttributeType.LONG) -> (i => Long.box(9007199254740993L + 2 * i)),
    new Attribute("a\"b\\c_region", AttributeType.STRING) -> (i => Seq("NA", "EU", "APAC")(i % 3)),
    new Attribute("a\"b\\c_nanos", AttributeType.TIMESTAMP) -> (i =>
      java.sql.Timestamp.valueOf(
        Seq(
          "2024-01-01 00:00:00.123456789",
          "2024-06-30 23:59:59.999999999",
          "2024-03-05 14:09:07.5"
        )(
          i % 3
        )
      )
    ),
    new Attribute("in1df", AttributeType.STRING) -> (i => Seq("x", "y", "z")(i % 3))
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

  /** Write the rows as a header-first CSV separated by `delimiter` and encoded in
    * `charset`.
    *
    * Both are parameters because they describe the BYTES, not the config: a variant
    * declaring `fileEncoding = UTF_16` over a file left in UTF-8, or a tab over a
    * file separated by commas, would compare nothing but how each path misreads it.
    *
    * Two headers are left blank, a timestamp's and a long's, which the reader
    * names column-N and pandas names "Unnamed: N" until it is told otherwise, so
    * a type asked for by the schema's name finds no column. The header at
    * position 1 is written as the text "Unnamed: 1", a name a user can really
    * choose and pandas' placeholder for the same place.
    */
  def writeCsv(dir: Path, charset: Charset, delimiter: Char, rows: Seq[Tuple]): Path = {
    val path = dir.resolve("sample.csv")
    val blank = Set("start_ts", "a\"b\\c_big")
    val header = schema.getAttributes.zipWithIndex
      .map {
        case (_, 1)                              => csvField("Unnamed: 1", delimiter)
        case (a, _) if blank.contains(a.getName) => ""
        case (a, _)                              => csvField(a.getName, delimiter)
      }
      .mkString(delimiter.toString)
    val body = rows.map { t =>
      schema.getAttributes
        .map(a => csvField(Option(t.getField[AnyRef](a.getName)).map(_.toString).orNull, delimiter))
        .mkString(delimiter.toString)
    }
    Files.write(path, ((header +: body).mkString("\n") + "\n").getBytes(charset))
    path
  }

  /** One CSV field, quoted per RFC 4180.
    *
    * The table carries commas inside values — a bracketed edge pair, a
    * comma-delimited list, an ordinary English sentence — and writing those raw
    * shifts every column after them. What the two paths then disagree about is a
    * broken file rather than anything either of them does. The same holds for
    * whichever character separates the fields: under a space, the sentence needs
    * its quotes as much as the list does under a comma.
    */
  private def csvField(value: String, delimiter: Char): String =
    if (value == null) ""
    else if (value.exists(c => c == delimiter || c == '"' || c == '\n' || c == '\r'))
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
