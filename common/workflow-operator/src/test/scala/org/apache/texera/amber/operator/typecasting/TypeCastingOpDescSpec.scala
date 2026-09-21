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

package org.apache.texera.amber.operator.typecasting

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, AttributeTypeUtils, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class TypeCastingOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def castUnit(attr: String, to: AttributeType): TypeCastingUnit = {
    val u = new TypeCastingUnit()
    u.attribute = attr
    u.resultType = to
    u
  }

  "TypeCastingOpDesc.operatorInfo" should "advertise the name and Cleaning group" in {
    val info = (new TypeCastingOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Type Casting"
    info.operatorGroupName shouldBe OperatorGroupConstants.CLEANING_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "TypeCastingOpDesc.getPhysicalOp" should "wire TypeCastingOpExec and carry port identities" in {
    val op = new TypeCastingOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.typecasting.TypeCastingOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "TypeCastingOpDesc schema propagation" should
    "leave the schema unchanged when there are no casting units" in {
    val op = new TypeCastingOpDesc
    val input = Schema().add(new Attribute("n", AttributeType.INTEGER))
    val out = op.getExternalOutputSchemas(Map(op.operatorInfo.inputPorts.head.id -> input))
    out shouldBe Map(op.operatorInfo.outputPorts.head.id -> input)
  }

  it should "change the target column's type for a casting unit" in {
    val op = new TypeCastingOpDesc
    op.typeCastingUnits = List(castUnit("n", AttributeType.STRING))
    val input = Schema().add(new Attribute("n", AttributeType.INTEGER))
    val out = op.getExternalOutputSchemas(Map(op.operatorInfo.inputPorts.head.id -> input))
    out shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add(new Attribute("n", AttributeType.STRING))
    )
  }

  "TypeCastingOpDesc" should "round-trip its casting units through the polymorphic base" in {
    val op = new TypeCastingOpDesc
    op.typeCastingUnits = List(castUnit("n", AttributeType.STRING))
    val restored =
      objectMapper.readValue(objectMapper.writeValueAsString(op), classOf[LogicalOp])
    restored shouldBe a[TypeCastingOpDesc]
    val tc = restored.asInstanceOf[TypeCastingOpDesc]
    tc.typeCastingUnits should have size 1
    tc.typeCastingUnits.head.attribute shouldBe "n"
    tc.typeCastingUnits.head.resultType shouldBe AttributeType.STRING
  }

  // The values a cast reads differently on the two sides. Python's own `bool`
  // answers true for every non-empty string, so "false" and "0" are where the
  // script used to disagree with the run it came from; text that is neither a
  // boolean nor a number, and an empty cell, are the two ends of the range.
  private val boolCases = Seq("true", "false", "0", "1", "not a boolean", null)

  /** What the engine answers, as the string the Python side prints back: the
    * literal, or `error` for a value `parseField` refuses.
    */
  private def engineAnswer(value: Any, to: AttributeType): String =
    Try(AttributeTypeUtils.parseField(value, to))
      .map(v => if (v == null) "null" else v.toString)
      .getOrElse("error")

  it should "cast to boolean the way AttributeTypeUtils does" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val op = new TypeCastingOpDesc
    op.typeCastingUnits = List(castUnit("v", AttributeType.BOOLEAN))

    // The generated block reads `in1df` and writes `out1df`, so it becomes the
    // body of a function the driver calls once per value. One row at a time,
    // because a value the cast refuses would otherwise end the comparison at
    // the first one.
    val body = op.generateStandaloneCode().linesIterator.map("    " + _).mkString("\n")
    val values = boolCases
      .map(v => if (v == null) "None" else "\"" + v + "\"")
      .mkString("[", ", ", "]")
    val driver =
      s"""import pandas as pd
         |
         |${op.standaloneHelpers().mkString("\n\n")}
         |
         |
         |def cast(in1df):
         |$body
         |    return out1df
         |
         |
         |for value in $values:
         |    try:
         |        answer = cast(pd.DataFrame({"v": [value]}))["v"].iloc[0]
         |        print("null" if pd.isna(answer) else str(answer).lower())
         |    except Exception:
         |        print("error")
         |""".stripMargin

    val script = Files.createTempFile("typecast-bool-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))

    val process =
      new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n${Source.fromFile(script.toFile).mkString}") {
      process.exitValue() shouldBe 0
    }

    val fromScript = out.trim.linesIterator.toSeq
    val fromEngine = boolCases.map { v =>
      if (v == null) "null" else engineAnswer(v, AttributeType.BOOLEAN).toLowerCase
    }
    withClue(s"cases=${boolCases.mkString(", ")}\nscript said $fromScript\n") {
      fromScript shouldBe fromEngine
    }
    // The pairs that made the review: "false" is not true, and "0" is not true.
    fromEngine shouldBe Seq("true", "false", "false", "true", "error", "null")
  }

  // One column per source type, since what the text looks like follows the
  // column and not the value: a whole double keeps its point, an integer never
  // grows one, and a boolean is lower case.
  private val stringColumns: Seq[(String, String, Seq[AnyRef])] = Seq(
    ("dbl", "float64", Seq(Double.box(6.0), Double.box(7.25))),
    ("int", "int64", Seq(Int.box(6), Int.box(7))),
    ("flag", "bool", Seq(Boolean.box(true), Boolean.box(false)))
  )

  it should "cast to string the way AttributeTypeUtils does" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val op = new TypeCastingOpDesc
    op.typeCastingUnits = stringColumns.map {
      case (name, _, _) => castUnit(name, AttributeType.STRING)
    }.toList

    val frame = stringColumns
      .map {
        case (name, dtype, values) =>
          val cells = values
            .map {
              case b: java.lang.Boolean => if (b) "True" else "False"
              case other                => other.toString
            }
            .mkString("[", ", ", "]")
          s"""    "$name": pd.Series($cells, dtype="$dtype"),"""
      }
      .mkString("\n")
    val driver =
      s"""import pandas as pd
         |
         |${op.standaloneHelpers().mkString("\n\n")}
         |
         |
         |in1df = pd.DataFrame({
         |$frame
         |})
         |${op.generateStandaloneCode()}
         |for column in ${stringColumns.map(c => "\"" + c._1 + "\"").mkString("[", ", ", "]")}:
         |    for cell in out1df[column]:
         |        print("null" if pd.isna(cell) else cell)
         |""".stripMargin

    val script = Files.createTempFile("typecast-string-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))

    val process =
      new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
    }

    val fromEngine = stringColumns.flatMap(_._3).map(v => engineAnswer(v, AttributeType.STRING))
    withClue(s"script said ${out.trim.linesIterator.toSeq}\n") {
      out.trim.linesIterator.toSeq shouldBe fromEngine
    }
    // The pair that made the review: a double keeps its point at 6.0, where the
    // integer 6 has none.
    fromEngine shouldBe Seq("6.0", "7.25", "6", "7", "true", "false")
  }

  // The moments where the two sides used to part. pandas parses into nanoseconds
  // by default, which reach only 1677 to 2262, so everything past that edge was
  // emptied where the engine holds a java.sql.Timestamp and reads it like any
  // other moment. The first three rows are ordinary ones, and they are also three
  // different formats in one column: the engine hands DateParserUtils a field at
  // a time, so a row states its own format rather than the column's first one.
  // Two of them state an offset, which DateParserUtils reads and java.sql.Timestamp
  // then keeps no zone for: the moment is held as the wall clock of the machine's
  // own zone. The expectation is taken from the engine rather than written down,
  // so the pair says the same thing wherever the suite runs.
  private val timestampCases = Seq(
    "2024-03-05 14:09:07",
    "2024-03-05T14:09:07",
    "March 5, 2024",
    "2024-03-05T14:09:07Z",
    "2024-03-05T14:09:07+05:30",
    "1677-09-22 00:12:44",
    "2262-04-11 23:47:16",
    "2500-01-01 00:00:00",
    "1500-06-15 08:30:00",
    "9999-12-31 23:59:59"
  )

  /** `java.sql.Timestamp.toString` always writes a fraction where Python writes
    * one only when there is something to write, and every case here lands on a
    * whole second.
    */
  private def withoutFraction(text: String): String = text.stripSuffix(".0")

  /** The cells the driver printed, told apart from anything pandas wrote to
    * stderr, which this process merges into the same stream.
    */
  private def cellsOf(out: String): Seq[String] =
    out.linesIterator.filter(_.startsWith("cell ")).map(_.drop("cell ".length)).toSeq

  it should "cast text to a timestamp the way AttributeTypeUtils does" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val op = new TypeCastingOpDesc
    op.typeCastingUnits = List(castUnit("v", AttributeType.TIMESTAMP))

    // One frame rather than a row at a time, unlike the casts that refuse a
    // value: this one coerces, and the column is where a format that is not the
    // first row's would be lost.
    val values = timestampCases.map(v => "\"" + v + "\"").mkString("[", ", ", "]")
    val driver =
      s"""import pandas as pd
         |
         |${op.standaloneHelpers().mkString("\n\n")}
         |
         |
         |in1df = pd.DataFrame({"v": $values})
         |${op.generateStandaloneCode()}
         |
         |for answer in out1df["v"]:
         |    print("cell", "null" if pd.isna(answer) else str(answer))
         |""".stripMargin

    val script = Files.createTempFile("typecast-timestamp-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))

    val process =
      new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") { process.exitValue() shouldBe 0 }

    // Only the printed cells: pandas writes a parsing warning to stderr, which
    // this process merges into the same stream.
    val fromScript = cellsOf(out)
    val fromEngine =
      timestampCases.map(v => withoutFraction(engineAnswer(v, AttributeType.TIMESTAMP)))
    withClue(s"cases=${timestampCases.mkString(", ")}\nscript said $fromScript\n") {
      fromScript shouldBe fromEngine
    }
    // The rows that made the issue: a moment either side of the nanosecond edge.
    fromEngine.takeRight(3) shouldBe
      Seq("2500-01-01 00:00:00", "1500-06-15 08:30:00", "9999-12-31 23:59:59")
    // And the pair that states an offset, which reaches the same moment by two
    // spellings: five and a half hours apart in the text, and so in the reading.
    val zoned = fromScript.slice(3, 5)
    java.time.Duration
      .between(
        java.time.LocalDateTime.parse(zoned(1).replace(' ', 'T')),
        java.time.LocalDateTime.parse(zoned(0).replace(' ', 'T'))
      )
      .toMinutes shouldBe 330
  }

  // A column that is already a moment is not text and is not re-read: parseField
  // hands a java.sql.Timestamp back untouched, and that class counts nanoseconds,
  // so narrowing the column here would fold two moments the run tells apart into
  // one. The pair below differs only past the microsecond.
  private val nanosecondCases = Seq(
    "2024-03-05 14:09:07.123456789",
    "2024-03-05 14:09:07.123456001"
  )

  it should "keep the resolution a timestamp column arrived in" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val op = new TypeCastingOpDesc
    op.typeCastingUnits = List(castUnit("v", AttributeType.TIMESTAMP))
    // The declared type is what sends this down the branch under test: a column
    // the engine already holds as a moment, cast to the type it already has.
    val input = Schema().add(new Attribute("v", AttributeType.TIMESTAMP))
    val generated = op.generateStandaloneCode(Map(op.operatorInfo.inputPorts.head.id -> input))

    val values = nanosecondCases.map(v => "\"" + v + "\"").mkString("[", ", ", "]")
    val driver =
      s"""import pandas as pd
         |
         |${op.standaloneHelpers().mkString("\n\n")}
         |
         |
         |in1df = pd.DataFrame({"v": pd.to_datetime($values)})
         |$generated
         |
         |for answer in out1df["v"]:
         |    print("cell", "null" if pd.isna(answer) else str(answer))
         |""".stripMargin

    val script = Files.createTempFile("typecast-timestamp-nanos-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))

    val process =
      new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") { process.exitValue() shouldBe 0 }

    val fromScript = cellsOf(out)
    val fromEngine =
      nanosecondCases.map(v => engineAnswer(java.sql.Timestamp.valueOf(v), AttributeType.TIMESTAMP))
    withClue(s"script said $fromScript\n") { fromScript shouldBe fromEngine }
    // What the two answers have to carry: the rows stay apart.
    fromEngine shouldBe nanosecondCases
  }

  // The one place the script is meant to differ, and the reason it cannot simply
  // parse strictly: the engine accepts a set of formats no single pandas call
  // states, so text neither can read leaves an empty cell instead of ending an
  // exported run halfway.
  it should "leave a cell it cannot read empty rather than refusing it" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val op = new TypeCastingOpDesc
    op.typeCastingUnits = List(castUnit("v", AttributeType.TIMESTAMP))
    val driver =
      s"""import pandas as pd
         |
         |${op.standaloneHelpers().mkString("\n\n")}
         |
         |
         |in1df = pd.DataFrame({"v": ["not a date", None, "2024-03-05 14:09:07"]})
         |${op.generateStandaloneCode()}
         |
         |for answer in out1df["v"]:
         |    print("cell", "null" if pd.isna(answer) else str(answer))
         |""".stripMargin

    val script = Files.createTempFile("typecast-timestamp-unreadable-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))

    val process =
      new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      cellsOf(out) shouldBe Seq("null", "null", "2024-03-05 14:09:07")
    }
    // The engine refuses the same text, which is the difference this coercion is.
    engineAnswer("not a date", AttributeType.TIMESTAMP) shouldBe "error"
  }

  // Python resolution follows FilledAreaPlotOpDescSpec: udf.conf python.path
  // (UDF_PYTHON_PATH), then python3 / python / py.
  private def resolvePython(): Option[String] = {
    def fromConfig: Option[String] =
      Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
        .orElse(Try(ConfigFactory.load()).toOption)
        .flatMap(c => Try(c.getConfig("python").getString("path")).toOption)
        .map(_.trim)
        .filter(_.nonEmpty)

    def runnable(exe: String): Boolean =
      Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start()).toOption
        .exists { p =>
          if (!p.waitFor(5, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
          else p.exitValue() == 0
        }

    (fromConfig.toList ++ List("python3", "python", "py")).distinct.find(runnable)
  }

  private def canImportPandas(python: String): Boolean =
    Try(
      new ProcessBuilder(python, "-c", "import pandas").redirectErrorStream(true).start()
    ).toOption
      .exists { p =>
        if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
        else p.exitValue() == 0
      }
}
