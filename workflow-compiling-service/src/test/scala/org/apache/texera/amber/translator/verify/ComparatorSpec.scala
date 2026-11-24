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
import org.apache.texera.amber.util.JSONUtils.objectMapper
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

  private def tempDir(suffix: String): Path =
    Files.createTempDirectory(s"comparator-spec-$suffix-")

  private def writeJsonl(dir: Path, name: String, rows: Seq[Tuple]): Path = {
    val p = dir.resolve(name)
    TupleIO.writeTuples(p, rows.iterator, schema)
    p
  }

  private def writeTyped(
      dir: Path,
      name: String,
      attrs: Seq[(String, AttributeType)],
      rows: Seq[Seq[AnyRef]]
  ): Path = {
    val typedSchema = attrs.foldLeft(Schema()) {
      case (s, (col, tpe)) => s.add(new Attribute(col, tpe))
    }
    val p = dir.resolve(name)
    val tuples = rows.map { values =>
      attrs
        .zip(values)
        .foldLeft(Tuple.builder(typedSchema)) {
          case (builder, ((col, _), value)) => builder.add(typedSchema.getAttribute(col), value)
        }
        .build()
    }
    TupleIO.writeTuples(p, tuples.iterator, typedSchema)
    p
  }

  // Written by hand rather than through TupleIO: these cases need a column set
  // the fixed schema above does not carry.
  private def writeLines(dir: Path, name: String, lines: Seq[String]): Path = {
    val p = dir.resolve(name)
    Files.writeString(p, lines.map(_ + "\n").mkString)
    p
  }

  private def assertEqualOk(
      actual: Path,
      expected: Path,
      orderSensitive: Boolean = true,
      modelColumns: Seq[String] = Seq.empty,
      probePath: Option[Path] = None
  ): Unit =
    noException should be thrownBy Comparator.assertEqual(
      actual,
      expected,
      orderSensitive = orderSensitive,
      modelColumns = modelColumns,
      probePath = probePath
    )

  private def assertMismatch(
      actual: Path,
      expected: Path,
      orderSensitive: Boolean = true,
      modelColumns: Seq[String] = Seq.empty,
      probePath: Option[Path] = None
  ): String =
    intercept[ComparatorMismatchException] {
      Comparator.assertEqual(
        actual,
        expected,
        orderSensitive = orderSensitive,
        modelColumns = modelColumns,
        probePath = probePath
      )
    }.getMessage

  "Comparator.assertEqual" should "pass when JSONL files contain identical rows" in {
    val dir = tempDir("equal")
    val rows = Seq(row(1, "alice"), row(2, "bob"))
    assertEqualOk(writeJsonl(dir, "a.jsonl", rows), writeJsonl(dir, "b.jsonl", rows))
  }

  it should "throw ComparatorMismatchException when JSONL files differ" in {
    val dir = tempDir("diff")
    assertMismatch(
      writeJsonl(dir, "a.jsonl", Seq(row(1, "alice"), row(2, "bob"))),
      writeJsonl(dir, "b.jsonl", Seq(row(1, "alice"), row(2, "carol")))
    )
  }

  it should "treat row-reordered files as unequal under positional comparison" in {
    val dir = tempDir("reorder-strict")
    assertMismatch(
      writeJsonl(dir, "a.jsonl", Seq(row(1, "alice"), row(2, "bob"))),
      writeJsonl(dir, "b.jsonl", Seq(row(2, "bob"), row(1, "alice")))
    )
  }

  it should "treat row-reordered files as equal under orderSensitive=false" in {
    val dir = tempDir("reorder-loose")
    assertEqualOk(
      writeJsonl(dir, "a.jsonl", Seq(row(1, "alice"), row(2, "bob"))),
      writeJsonl(dir, "b.jsonl", Seq(row(2, "bob"), row(1, "alice"))),
      orderSensitive = false
    )
  }

  it should "still report value mismatches under orderSensitive=false" in {
    // orderSensitive=false relaxes row ORDER, not row CONTENT — a genuinely
    // different cell must still fail.
    val dir = tempDir("reorder-content-diff")
    assertMismatch(
      writeJsonl(dir, "a.jsonl", Seq(row(1, "alice"), row(2, "bob"))),
      writeJsonl(dir, "b.jsonl", Seq(row(2, "bob"), row(1, "carol"))),
      orderSensitive = false
    )
  }

  // Sorting on the float first paired 1.0 with 1.0 across the two sides, which
  // put "a" beside "b". The exact column has to decide the pairing.
  it should "pair rows by their exact columns before a float under orderSensitive=false" in {
    val dir = tempDir("reorder-float")
    val a = writeLines(dir, "a.jsonl", Seq("""{"x":1.0,"s":"a"}""", """{"x":1.000001,"s":"b"}"""))
    val b = writeLines(dir, "b.jsonl", Seq("""{"x":1.000001,"s":"a"}""", """{"x":1.0,"s":"b"}"""))
    assertEqualOk(a, b, orderSensitive = false)
  }

  // With no exact column, sorting on x put 1.0 first on both sides, which put
  // y = 10.0 beside y = 100.0. Rows are paired within tolerance on every float.
  it should "pair rows within tolerance across every float under orderSensitive=false" in {
    val dir = tempDir("reorder-two-floats")
    val attrs = Seq("x" -> AttributeType.DOUBLE, "y" -> AttributeType.DOUBLE)
    def write(name: String, rows: Seq[(Double, Double)]): Path =
      writeTyped(dir, name, attrs, rows.map { case (x, y) => Seq(Double.box(x), Double.box(y)) })
    val a = write("a.jsonl", Seq((1.0, 10.0), (1.000001, 100.0)))
    assertEqualOk(a, write("b.jsonl", Seq((1.000001, 10.0), (1.0, 100.0))), orderSensitive = false)
    assertMismatch(a, write("c.jsonl", Seq((1.000001, 10.0), (1.0, 99.0))), orderSensitive = false)
  }

  // The tolerance a double needs was being applied to integers as well, so two
  // whole numbers a workflow would never call the same compared equal. The
  // declared type is what separates them: a double keeps the tolerance.
  private def writeLongs(dir: Path, name: String, values: Seq[java.lang.Long]): Path =
    writeTyped(dir, name, Seq("n" -> AttributeType.LONG), values.map(v => Seq[AnyRef](v)))

  it should "reject two integers the float tolerance would have accepted" in {
    val dir = tempDir("long-tolerance")
    assertMismatch(
      writeLongs(dir, "a.jsonl", Seq(100000L)),
      writeLongs(dir, "b.jsonl", Seq(100001L))
    )
  }

  it should "reject a nullable integer that only a rounding read made equal" in {
    // A null widens the column to float on the way into pandas, and 9007199254740993
    // is already 9007199254740992 before anything compares it.
    val dir = tempDir("long-precision")
    assertMismatch(
      writeLongs(dir, "a.jsonl", Seq(9007199254740993L, null)),
      writeLongs(dir, "b.jsonl", Seq(9007199254740992L, null))
    )
  }

  it should "keep the tolerance a double needs" in {
    val dir = tempDir("double-tolerance")
    val attrs = Seq("x" -> AttributeType.DOUBLE)
    assertEqualOk(
      writeTyped(dir, "a.jsonl", attrs, Seq(Seq(Double.box(1.000001)))),
      writeTyped(dir, "b.jsonl", attrs, Seq(Seq(Double.box(1.0000011))))
    )
  }

  it should "reject an integer column only one side widened to a float" in {
    // This passed on the grounds that 6.0 and 6 spell the same integer. They do
    // not survive the same: a later cast to text writes "6" from one and "6.0"
    // from the other.
    val dir = tempDir("int-spelling")
    val a = writeLongs(dir, "a.jsonl", Seq(6L))
    val b = writeLines(dir, "b.jsonl", Seq("""{"n":6.0}"""))
    assertMismatch(a, b) should include("the two sides wrote it differently")
  }

  it should "accept an integer column both sides widened alike" in {
    // Where the engine's own path is a Python operator its table goes through
    // pandas too, so a hole widens the column on both paths and the two still
    // agree. What is asked here is whether the paths match, not whether pandas
    // kept the type.
    val dir = tempDir("int-both-widened")
    // writeLongs for the schema sidecar it leaves; the rows are then rewritten
    // as the widened form both paths would have produced. The null is the hole
    // that widened it, and float64 has to hold it too.
    val a = writeLongs(dir, "a.jsonl", Seq(6L, null))
    Files.writeString(a, "{\"n\":6.0}\n{\"n\":null}\n")
    val b = writeLines(dir, "b.jsonl", Seq("""{"n":6.0}""", """{"n":null}"""))
    assertEqualOk(a, b)
  }

  // The comparison took its columns from the actual side and selected those out
  // of the expected one, so a column only the expected side carried was never
  // looked at.
  it should "report a column only one side carries" in {
    val dir = tempDir("extra-column")
    val a = writeLines(dir, "a.jsonl", Seq("""{"id":1,"name":"alice"}"""))
    val b = writeLines(dir, "b.jsonl", Seq("""{"id":1,"name":"alice","extra":9}"""))

    assertMismatch(a, b) should include("only in expected")
    assertMismatch(b, a) should include("only in actual")
  }

  // Column order is part of the schema: it decides what a positional UDF reads,
  // what a file export writes, and what code asking for the first column gets.
  it should "treat column-reordered files as unequal" in {
    val dir = tempDir("column-order")
    val a = writeLines(dir, "a.jsonl", Seq("""{"a":1,"b":2}"""))
    val b = writeLines(dir, "b.jsonl", Seq("""{"b":2,"a":1}"""))
    assertMismatch(a, b) should include("different order")
  }

  // Reading both sides with `dtype=str` settles them on one spelling, which a
  // text column needs, but it reads the JSON number 6 as "6" just as readily, so
  // a side that wrote a number where the engine declared text compared equal to
  // the text. What the file actually holds is what separates them.
  it should "reject a number written where the schema declares text" in {
    val dir = tempDir("text-number")
    val a = writeTyped(dir, "a.jsonl", Seq("code" -> AttributeType.STRING), Seq(Seq("6")))
    val b = writeLines(dir, "b.jsonl", Seq("""{"code":6}"""))

    assertMismatch(a, b) should include("declared text but holds 6")
  }

  // numpy counts True as 1 and the frame comparison is asked not to check
  // dtypes, so a column of booleans compared equal to a column of ones. The
  // engine says a BOOLEAN column holds true and false.
  it should "reject a number written where the schema declares a boolean" in {
    val dir = tempDir("bool-number")
    val a =
      writeTyped(dir, "a.jsonl", Seq("flag" -> AttributeType.BOOLEAN), Seq(Seq(Boolean.box(true))))
    val b = writeLines(dir, "b.jsonl", Seq("""{"flag":1}"""))

    assertMismatch(a, b) should include("the two sides wrote it differently")
  }

  it should "accept a boolean column both sides widened alike" in {
    // A hole costs a pandas boolean column its type on both paths alike, so
    // both write the true as 1.0 next to the null.
    val dir = tempDir("bool-both-widened")
    val a =
      writeTyped(dir, "a.jsonl", Seq("flag" -> AttributeType.BOOLEAN), Seq(Seq(Boolean.box(true))))
    Files.writeString(a, "{\"flag\":1.0}\n{\"flag\":null}\n")
    val b = writeLines(dir, "b.jsonl", Seq("""{"flag":1.0}""", """{"flag":null}"""))
    assertEqualOk(a, b)
  }

  // Both files are UTF-8. Read with the platform's encoding, a text value
  // outside ASCII failed to decode, and a LONG column whose name is outside
  // ASCII lost its declared type and went back under the float tolerance.
  it should "read a name and a value outside ASCII as the UTF-8 they are" in {
    val dir = tempDir("utf8")
    val attrs = Seq("é" -> AttributeType.LONG, "v" -> AttributeType.STRING)
    def write(name: String, n: Long): Path =
      writeTyped(dir, name, attrs, Seq(Seq(Long.box(n), "ひ")))
    val a = write("a.jsonl", 100000L)

    assertEqualOk(a, write("same.jsonl", 100000L))
    assertMismatch(a, write("b.jsonl", 100001L)) should include("100001")
  }

  // What kind of value each DOUBLE cell holds used to be added to the frame as
  // a column with a made-up name. An output column of that name was overwritten
  // on both sides alike, so a difference in it went unseen.
  it should "not let a DOUBLE column's kinds hide an output column" in {
    val dir = tempDir("double-kind-name")
    val attrs =
      Seq("v" -> AttributeType.DOUBLE, "v (null/NaN/±Infinity/-0.0)" -> AttributeType.STRING)
    val a = writeTyped(dir, "a.jsonl", attrs, Seq(Seq(Double.box(1.0), "before")))
    val b = writeTyped(dir, "b.jsonl", attrs, Seq(Seq(Double.box(1.0), "after")))

    assertMismatch(a, b) should include("after")
    assertMismatch(a, b, orderSensitive = false) should include("after")
  }

  it should "report a model column only one side produced" in {
    // Model columns are compared by behavior and then dropped from both frames,
    // so a side that never wrote one has to fail here: once the column is gone,
    // the frame diff sees two identical column sets and passes.
    val dir = tempDir("model-missing")
    val withModel = writeLines(dir, "with-model.jsonl", Seq("""{"model":"eA==","score":1.0}"""))
    val withoutModel = writeLines(dir, "without-model.jsonl", Seq("""{"score":1.0}"""))
    val probe = writeLines(dir, "probe.jsonl", Seq("""{"petal_length":1.0,"label":0}"""))

    assertMismatch(
      withModel,
      withoutModel,
      modelColumns = Seq("model"),
      probePath = Some(probe)
    ) should
      include("missing from expected")
    assertMismatch(
      withoutModel,
      withModel,
      modelColumns = Seq("model"),
      probePath = Some(probe)
    ) should
      include("missing from actual")
  }

  /** A model column holds a pickled estimator, which only Python can fit. Writes
    * the two one-row frames the comparison reads and the probe it predicts on,
    * each side's model fitted on its own targets and its own feature column.
    * The probe carries both columns, with the same values.
    */
  private def writeFittedModels(
      dir: Path,
      estimator: String,
      actualTargets: String,
      expectedTargets: String,
      actualFeature: String = "petal_length",
      expectedFeature: String = "petal_length"
  ): (Path, Path, Path) = {
    val program =
      s"""import base64, json, pickle
         |import pandas as pd
         |from sklearn.tree import $estimator as Estimator
         |
         |x = pd.DataFrame({"petal_length": [1.0, 2.0, 3.0], "petal_width": [1.0, 2.0, 3.0]})
         |x.to_json(r"$dir/probe.jsonl", orient="records", lines=True)
         |for name, y, feature in (
         |    ("actual.jsonl", $actualTargets, "$actualFeature"),
         |    ("expected.jsonl", $expectedTargets, "$expectedFeature"),
         |):
         |    model = base64.b64encode(pickle.dumps(Estimator().fit(x[[feature]], y))).decode()
         |    with open(rf"$dir/{name}", "w", encoding="utf-8") as handle:
         |        handle.write(json.dumps({"model": model, "score": 1.0}) + "\\n")
         |""".stripMargin
    runPython(program)
    (dir.resolve("actual.jsonl"), dir.resolve("expected.jsonl"), dir.resolve("probe.jsonl"))
  }

  private def runPythonScript(script: Path, args: String*): (Int, String) = {
    val python = sys.env.get("UDF_PYTHON_PATH").filter(_.nonEmpty).getOrElse("python3")
    val process = new ProcessBuilder((python +: script.toString +: args): _*)
      .redirectErrorStream(true)
      .start()
    val said = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    (process.exitValue(), said)
  }

  private def runPython(program: String): Unit = {
    val script = Files.createTempFile("comparator-spec-fit-", ".py")
    script.toFile.deleteOnExit()
    Files.writeString(script, program)
    val (exit, said) = runPythonScript(script)
    require(exit == 0, s"fitting the models failed:\n$said")
  }

  // A classifier predicts a class, and a class that changed is a different
  // answer however near the two labels sit. Numbered classes went through the
  // numeric tolerance, where 100000 and 100001 are one part in 1e5 apart and
  // compared equal, so every row's class could change and the models still pass.
  it should "catch a classifier whose predicted classes all changed" in {
    val dir = tempDir("model-classes")
    val (actual, expected, probe) =
      writeFittedModels(dir, "DecisionTreeClassifier", "[100000] * 3", "[100001] * 3")
    assertMismatch(actual, expected, modelColumns = Seq("model"), probePath = Some(probe)) should
      include("predictions differ")
  }

  // What a model was fitted on is part of the model. Two fitted on different
  // columns answer alike on a probe carrying both, so only the names part them.
  it should "catch two models fitted on different columns that predict alike" in {
    val dir = tempDir("model-features")
    val (actual, expected, probe) =
      writeFittedModels(
        dir,
        "DecisionTreeClassifier",
        "[0, 1, 1]",
        "[0, 1, 1]",
        expectedFeature = "petal_width"
      )
    assertMismatch(actual, expected, modelColumns = Seq("model"), probePath = Some(probe)) should
      include("fitted feature names differ")
  }

  // The tolerance still has to reach what it was for: a regressor computes its
  // answer, so the two paths can land a float's last bits apart on the same fit.
  it should "allow a regressor's predictions to differ within tolerance" in {
    val dir = tempDir("model-regressor")
    val (actual, expected, probe) =
      writeFittedModels(
        dir,
        "DecisionTreeRegressor",
        "[1.0, 2.0, 3.0]",
        "[1.0, 2.0, 3.0 + 1e-12]"
      )
    assertEqualOk(actual, expected, modelColumns = Seq("model"), probePath = Some(probe))
  }

  // Unordered rows were paired only after the models had been compared by
  // position, so a model could change rows and still pass.
  it should "keep each model beside its row under orderSensitive=false" in {
    val dir = tempDir("model-unordered")
    runPython(
      s"""import base64, json, pickle
         |import pandas as pd
         |from sklearn.tree import DecisionTreeClassifier
         |
         |x = pd.DataFrame({"petal_length": [1.0, 2.0, 3.0]})
         |x.to_json(r"$dir/probe.jsonl", orient="records", lines=True)
         |def predicting(c):
         |    model = DecisionTreeClassifier().fit(x, [c] * 3)
         |    return base64.b64encode(pickle.dumps(model)).decode()
         |for name, rows in (
         |    ("rows.jsonl", [(1, 0), (2, 1)]),
         |    ("reversed.jsonl", [(2, 1), (1, 0)]),
         |    ("swapped.jsonl", [(2, 0), (1, 1)]),
         |    ("tied.jsonl", [(1, 0), (1, 1)]),
         |    ("tied-reversed.jsonl", [(1, 1), (1, 0)]),
         |    ("tied-changed.jsonl", [(1, 1), (1, 1)]),
         |):
         |    with open(rf"$dir/{name}", "w", encoding="utf-8") as handle:
         |        for id, c in rows:
         |            handle.write(json.dumps({"id": id, "model": predicting(c)}) + "\\n")
         |""".stripMargin
    )
    def compare(expected: String, actual: String = "rows.jsonl"): Unit =
      Comparator.assertEqual(
        dir.resolve(actual),
        dir.resolve(expected),
        orderSensitive = false,
        modelColumns = Seq("model"),
        probePath = Some(dir.resolve("probe.jsonl"))
      )

    noException should be thrownBy compare("reversed.jsonl")
    intercept[ComparatorMismatchException] {
      compare("swapped.jsonl")
    }.getMessage should include("predictions differ")
    // Two rows with the same id tie on every sorted column, so the sort kept
    // them in file order and compared their models the wrong way round.
    noException should be thrownBy compare("tied-reversed.jsonl", "tied.jsonl")
    intercept[ComparatorMismatchException] {
      compare("tied-changed.jsonl", "tied.jsonl")
    }.getMessage should include("predictions differ")
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
    runPythonScript(script, "--plotly", actual.toString, expected.toString)
  }

  private def writePlotlyRows(dir: Path, values: Int*): Path =
    writeLines(dir, "actual.jsonl", values.map(v => s"""{"json-content": ${plotlyFigure(v)}}"""))

  private def writePlotlyArray(dir: Path, values: Int*): Path =
    writeLines(dir, "expected.json", Seq(values.map(plotlyFigure).mkString("[", ",", "]")))

  private def assertPlotlyPass(actual: Path, expected: Path): Unit = {
    val (exit, said) = runPlotly(actual, expected)
    withClue(s"compare.py said:\n$said") { exit shouldBe 0 }
  }

  private def assertPlotlyFail(actual: Path, expected: Path, expectedSnippets: String*): Unit = {
    val (exit, said) = runPlotly(actual, expected)
    withClue(s"compare.py said:\n$said") {
      exit should not be 0
      expectedSnippets.foreach(said should include(_))
    }
  }

  "Comparator's plotly comparison" should "read every chart on both sides" in {
    val dir = tempDir("plotly-all")
    assertPlotlyPass(writePlotlyRows(dir, 1, 2, 3), writePlotlyArray(dir, 1, 2, 3))
  }

  it should "catch a chart that differs after the first" in {
    val dir = tempDir("plotly-second")
    assertPlotlyFail(writePlotlyRows(dir, 1, 2, 3), writePlotlyArray(dir, 1, 99, 3), "chart 2 of 3")
  }

  it should "catch an exported script that drew fewer charts than the run" in {
    val dir = tempDir("plotly-count")
    assertPlotlyFail(
      writePlotlyRows(dir, 1, 2, 3),
      writeLines(dir, "expected.json", Seq(plotlyFigure(1))),
      "drew 3",
      "drew 1"
    )
  }

  // The operators that draw one chart write a lone figure, and there are far
  // more of those than there are of the other kind.
  it should "still read a lone figure as the one chart it is" in {
    val dir = tempDir("plotly-one")
    assertPlotlyPass(
      writePlotlyRows(dir, 7),
      writeLines(dir, "expected.json", Seq(plotlyFigure(7)))
    )
  }

  // A bullet or gauge chart draws its charts onto one page and yields that page
  // as a single row. Every call on a page is a chart to compare, whichever side
  // the page is on.
  private def plotlyPage(values: Int*): String =
    values
      .map(v =>
        s"""<div><script>Plotly.newPlot("div$v", """ +
          s"""[{"type": "indicator", "value": $v}], {}, {"responsive": true})</script></div>"""
      )
      .mkString

  /** A page as the runtime path hands it over: one JSONL row, markup escaped. */
  private def plotlyPageRow(values: Int*): String = {
    val node = objectMapper.createObjectNode()
    node.put("html-content", plotlyPage(values: _*))
    objectMapper.writeValueAsString(node)
  }

  it should "read every chart on a page the run drew as one row" in {
    val dir = tempDir("plotly-page")
    assertPlotlyPass(
      writeLines(dir, "actual.jsonl", Seq(plotlyPageRow(1, 2, 3))),
      writeLines(dir, "expected.html", Seq(plotlyPage(1, 2, 3)))
    )
  }

  it should "catch a chart after the first on a page the script wrote" in {
    val dir = tempDir("plotly-page-second")
    assertPlotlyFail(
      writeLines(dir, "actual.jsonl", Seq(plotlyPageRow(1, 2, 3))),
      writeLines(dir, "expected.html", Seq(plotlyPage(1, 99, 3))),
      "chart 2 of 3"
    )
  }

  // `bool` is a subclass of `int` in Python, so a boolean setting fell into the
  // numeric branch and True compared equal to 1. A column whose declared type
  // changed from boolean to integer reaches a figure as exactly that.
  it should "treat a boolean setting and the number 1 as different" in {
    val dir = tempDir("plotly-bool")
    val actual = writeLines(
      dir,
      "actual.jsonl",
      Seq("""{"json-content": {"data": [{"visible": true}], "layout": {}}}""")
    )
    def expected(visible: String): Path =
      writeLines(
        dir,
        s"expected-$visible.json",
        Seq(s"""{"data": [{"visible": $visible}], "layout": {}}""")
      )

    assertPlotlyFail(actual, expected("1"))
    assertPlotlyPass(actual, expected("true"))
  }
}
