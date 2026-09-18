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
import org.apache.texera.amber.translator.verify.tags.IntegrationTest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, StandardCopyOption}
import java.util.concurrent.TimeUnit
import scala.io.Source

// Tagged @IntegrationTest: Comparator.assertEqual shells out to compare.py, so
// this spec needs Python and must run in the Python-provisioned integration job.
@IntegrationTest
class ComparatorSpec extends AnyFlatSpec with Matchers {

  private val schema: Schema = Schema()
    .add(new Attribute("id", AttributeType.INTEGER))
    .add(new Attribute("name", AttributeType.STRING))

  private val idAttr = new Attribute("id", AttributeType.INTEGER)
  private val nameAttr = new Attribute("name", AttributeType.STRING)

  private def row(id: Int, name: String): Tuple =
    Tuple
      .builder(schema)
      .add(idAttr, Int.box(id))
      .add(nameAttr, name)
      .build()

  private def writeJsonl(dir: Path, name: String, rows: Seq[Tuple]): Path = {
    val p = dir.resolve(name)
    TupleIO.writeTuples(p, rows.iterator, schema)
    p
  }

  // Written by hand rather than through TupleIO: these cases need a column set
  // the fixed schema above does not carry.
  private def writeLines(dir: Path, name: String, lines: Seq[String]): Path = {
    val p = dir.resolve(name)
    Files.writeString(p, lines.map(_ + "\n").mkString)
    p
  }

  "Comparator.assertEqual" should "pass when JSONL files contain identical rows" in {
    val dir = Files.createTempDirectory("comparator-spec-equal-")
    val rows = Seq(row(1, "alice"), row(2, "bob"))
    val a = writeJsonl(dir, "a.jsonl", rows)
    val b = writeJsonl(dir, "b.jsonl", rows)
    noException should be thrownBy Comparator.assertEqual(a, b)
  }

  it should "throw ComparatorMismatchException when JSONL files differ" in {
    val dir = Files.createTempDirectory("comparator-spec-diff-")
    val a = writeJsonl(dir, "a.jsonl", Seq(row(1, "alice"), row(2, "bob")))
    val b = writeJsonl(dir, "b.jsonl", Seq(row(1, "alice"), row(2, "carol")))
    intercept[ComparatorMismatchException] {
      Comparator.assertEqual(a, b)
    }
  }

  it should "treat row-reordered files as unequal under positional comparison" in {
    val dir = Files.createTempDirectory("comparator-spec-reorder-strict-")
    val a = writeJsonl(dir, "a.jsonl", Seq(row(1, "alice"), row(2, "bob")))
    val b = writeJsonl(dir, "b.jsonl", Seq(row(2, "bob"), row(1, "alice")))
    intercept[ComparatorMismatchException] {
      Comparator.assertEqual(a, b)
    }
  }

  it should "treat row-reordered files as equal under orderSensitive=false" in {
    val dir = Files.createTempDirectory("comparator-spec-reorder-loose-")
    val a = writeJsonl(dir, "a.jsonl", Seq(row(1, "alice"), row(2, "bob")))
    val b = writeJsonl(dir, "b.jsonl", Seq(row(2, "bob"), row(1, "alice")))
    noException should be thrownBy Comparator.assertEqual(a, b, orderSensitive = false)
  }

  it should "still report value mismatches under orderSensitive=false" in {
    // orderSensitive=false relaxes row ORDER, not row CONTENT — a genuinely
    // different cell must still fail.
    val dir = Files.createTempDirectory("comparator-spec-reorder-content-diff-")
    val a = writeJsonl(dir, "a.jsonl", Seq(row(1, "alice"), row(2, "bob")))
    val b = writeJsonl(dir, "b.jsonl", Seq(row(2, "bob"), row(1, "carol")))
    intercept[ComparatorMismatchException] {
      Comparator.assertEqual(a, b, orderSensitive = false)
    }
  }

  // The tolerance a double needs was being applied to integers as well, so two
  // whole numbers a workflow would never call the same compared equal. The
  // declared type is what separates them: a double keeps the tolerance.
  private val longSchema: Schema = Schema().add(new Attribute("n", AttributeType.LONG))

  private def writeLongs(dir: Path, name: String, values: Seq[java.lang.Long]): Path = {
    val p = dir.resolve(name)
    val rows =
      values.map(v => Tuple.builder(longSchema).add(longSchema.getAttribute("n"), v).build())
    TupleIO.writeTuples(p, rows.iterator, longSchema)
    p
  }

  it should "reject two integers the float tolerance would have accepted" in {
    val dir = Files.createTempDirectory("comparator-spec-long-tolerance-")
    val a = writeLongs(dir, "a.jsonl", Seq(100000L))
    val b = writeLongs(dir, "b.jsonl", Seq(100001L))
    intercept[ComparatorMismatchException] {
      Comparator.assertEqual(a, b)
    }
  }

  it should "reject a nullable integer that only a rounding read made equal" in {
    // A null widens the column to float on the way into pandas, and 9007199254740993
    // is already 9007199254740992 before anything compares it.
    val dir = Files.createTempDirectory("comparator-spec-long-precision-")
    val a = writeLongs(dir, "a.jsonl", Seq(9007199254740993L, null))
    val b = writeLongs(dir, "b.jsonl", Seq(9007199254740992L, null))
    intercept[ComparatorMismatchException] {
      Comparator.assertEqual(a, b)
    }
  }

  it should "keep the tolerance a double needs" in {
    val dir = Files.createTempDirectory("comparator-spec-double-tolerance-")
    val doubleSchema = Schema().add(new Attribute("x", AttributeType.DOUBLE))
    def write(name: String, v: Double): Path = {
      val p = dir.resolve(name)
      val row =
        Tuple.builder(doubleSchema).add(doubleSchema.getAttribute("x"), Double.box(v)).build()
      TupleIO.writeTuples(p, Iterator(row), doubleSchema)
      p
    }
    noException should be thrownBy Comparator.assertEqual(
      write("a.jsonl", 1.000001),
      write("b.jsonl", 1.0000011)
    )
  }

  it should "read an integer column by its declared type on both sides" in {
    // The script widens a holed integer column to float and writes 6.0 where the
    // engine wrote 6. Both are the integer the schema declares, so this is the one
    // difference in spelling that is not a difference in answer.
    val dir = Files.createTempDirectory("comparator-spec-int-spelling-")
    val a = writeLongs(dir, "a.jsonl", Seq(6L))
    val b = writeLines(dir, "b.jsonl", Seq("""{"n":6.0}"""))
    noException should be thrownBy Comparator.assertEqual(a, b)
  }

  it should "report a model column only one side produced" in {
    // Model columns are compared by behavior and then dropped from both frames,
    // so a side that never wrote one has to fail here: once the column is gone,
    // the frame diff sees two identical column sets and passes.
    val dir = Files.createTempDirectory("comparator-spec-model-missing-")
    val withModel = writeLines(dir, "with-model.jsonl", Seq("""{"model":"eA==","score":1.0}"""))
    val withoutModel = writeLines(dir, "without-model.jsonl", Seq("""{"score":1.0}"""))
    val probe = writeLines(dir, "probe.jsonl", Seq("""{"petal_length":1.0,"label":0}"""))

    intercept[ComparatorMismatchException] {
      Comparator.assertEqual(
        withModel,
        withoutModel,
        modelColumns = Seq("model"),
        probePath = Some(probe)
      )
    }.getMessage should include("missing from expected")

    intercept[ComparatorMismatchException] {
      Comparator.assertEqual(
        withoutModel,
        withModel,
        modelColumns = Seq("model"),
        probePath = Some(probe)
      )
    }.getMessage should include("missing from actual")
  }

  // An operator that draws a chart per row writes one JSONL row per chart on the
  // runtime side and an array on the exported one. Reading a single figure from
  // each, as this did, left every chart after the first unread.
  private def plotlyFigure(value: Int): String =
    s"""{"data": [{"type": "indicator", "value": $value}], "layout": {}}"""

  /** compare.py --plotly, as its own process: the exit code and what it said. */
  private def runPlotly(actual: Path, expected: Path): (Int, String) = {
    val script = Files.createTempFile("compare-", ".py")
    script.toFile.deleteOnExit()
    val stream = getClass.getResourceAsStream("/python/compare.py")
    require(stream != null, "compare.py not found at /python/compare.py")
    try Files.copy(stream, script, StandardCopyOption.REPLACE_EXISTING)
    finally stream.close()

    val python = sys.env.get("UDF_PYTHON_PATH").filter(_.nonEmpty).getOrElse("python3")
    val process = new ProcessBuilder(
      python,
      script.toString,
      "--plotly",
      actual.toString,
      expected.toString
    ).redirectErrorStream(true).start()
    val said = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    (process.exitValue(), said)
  }

  "Comparator's plotly comparison" should "read every chart on both sides" in {
    val dir = Files.createTempDirectory("comparator-spec-plotly-all-")
    val actual = writeLines(
      dir,
      "actual.jsonl",
      Seq(1, 2, 3).map(v => s"""{"json-content": ${plotlyFigure(v)}}""")
    )
    val expected = writeLines(
      dir,
      "expected.json",
      Seq(Seq(1, 2, 3).map(plotlyFigure).mkString("[", ",", "]"))
    )
    val (exit, said) = runPlotly(actual, expected)
    withClue(s"compare.py said:\n$said") { exit shouldBe 0 }
  }

  it should "catch a chart that differs after the first" in {
    val dir = Files.createTempDirectory("comparator-spec-plotly-second-")
    val actual = writeLines(
      dir,
      "actual.jsonl",
      Seq(1, 2, 3).map(v => s"""{"json-content": ${plotlyFigure(v)}}""")
    )
    val expected = writeLines(
      dir,
      "expected.json",
      Seq(Seq(1, 99, 3).map(plotlyFigure).mkString("[", ",", "]"))
    )
    val (exit, said) = runPlotly(actual, expected)
    withClue(s"compare.py said:\n$said") {
      exit should not be 0
      said should include("chart 2 of 3")
    }
  }

  it should "catch an exported script that drew fewer charts than the run" in {
    val dir = Files.createTempDirectory("comparator-spec-plotly-count-")
    val actual = writeLines(
      dir,
      "actual.jsonl",
      Seq(1, 2, 3).map(v => s"""{"json-content": ${plotlyFigure(v)}}""")
    )
    val expected = writeLines(dir, "expected.json", Seq(plotlyFigure(1)))
    val (exit, said) = runPlotly(actual, expected)
    withClue(s"compare.py said:\n$said") {
      exit should not be 0
      said should include("drew 3")
      said should include("drew 1")
    }
  }

  // The operators that draw one chart write a lone figure, and there are far
  // more of those than there are of the other kind.
  it should "still read a lone figure as the one chart it is" in {
    val dir = Files.createTempDirectory("comparator-spec-plotly-one-")
    val actual = writeLines(dir, "actual.jsonl", Seq(s"""{"json-content": ${plotlyFigure(7)}}"""))
    val expected = writeLines(dir, "expected.json", Seq(plotlyFigure(7)))
    val (exit, said) = runPlotly(actual, expected)
    withClue(s"compare.py said:\n$said") { exit shouldBe 0 }
  }
}
