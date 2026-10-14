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
import org.apache.texera.amber.operator.tags.IntegrationTest
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.Tag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class TypeCastingOpDescSpec extends AnyFlatSpec with Matchers {

  private val NeedsPythonPackages = Tag(classOf[IntegrationTest].getName)

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

  // Text that is neither a boolean nor a number, which the engine refuses. The
  // verification only compares runs that both produced something, so it cannot
  // ask this; the values both sides read are its `str_to_bool` column.
  private val boolCases = Seq("not a boolean")

  /** What the engine answers, as the string the Python side prints back: the
    * literal, or `error` for a value `parseField` refuses.
    */
  private def engineAnswer(value: Any, to: AttributeType): String =
    Try(AttributeTypeUtils.parseField(value, to))
      .map(v => if (v == null) "null" else v.toString)
      .getOrElse("error")

  it should "refuse a boolean the way AttributeTypeUtils does" taggedAs NeedsPythonPackages in {
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
    fromEngine shouldBe Seq("error")
  }

  // A double column read from Arrow into a nullable float keeps a stored NaN and
  // an empty cell apart. The engine casts a NaN like any other double: "NaN" as
  // text, 0 as an integer, true as a boolean. The infinities and 3e9 sit past
  // the integer bounds, which a Double's toInt and toLong saturate at.
  private val doubleCases: Seq[java.lang.Double] =
    Seq(
      Double.NaN,
      null,
      1.0,
      -2.7,
      3.0e9,
      Double.PositiveInfinity,
      Double.NegativeInfinity,
      -0.0
    )

  private val doubleTargets =
    Seq(AttributeType.STRING, AttributeType.INTEGER, AttributeType.LONG, AttributeType.BOOLEAN)

  it should "cast a stored NaN the way AttributeTypeUtils does" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    // One column per target, each holding the same doubles.
    val columns = doubleTargets.map(_.name().toLowerCase)
    val op = new TypeCastingOpDesc
    op.typeCastingUnits = doubleTargets.zip(columns).map { case (to, c) => castUnit(c, to) }.toList
    val input = columns.foldLeft(Schema())((s, c) => s.add(new Attribute(c, AttributeType.DOUBLE)))
    val generated = op.generateStandaloneCode(Map(op.operatorInfo.inputPorts.head.id -> input))

    val values = doubleCases
      .map {
        case null                => "None"
        case d if d.isNaN        => "float(\"nan\")"
        case d if d.isInfinite() => if (d > 0) "float(\"inf\")" else "float(\"-inf\")"
        case d                   => d.toString
      }
      .mkString("[", ", ", "]")
    // Read the way the Arrow source reads a file, so the column arrives with its
    // validity mask and not with NaN standing in for every empty cell.
    val driver =
      s"""import pandas as pd
         |import pyarrow as pa
         |
         |${op.standaloneHelpers().mkString("\n\n")}
         |
         |
         |_columns = ${columns.map("\"" + _ + "\"").mkString("[", ", ", "]")}
         |_table = pa.table({c: pa.array($values, pa.float64()) for c in _columns})
         |in1df = _table.to_pandas(types_mapper={pa.float64(): pd.Float64Dtype()}.get)
         |$generated
         |
         |for c in _columns:
         |    for answer in out1df[c]:
         |        if answer is None or answer is pd.NA:
         |            answer = "null"
         |        elif pd.api.types.is_bool(answer):
         |            answer = str(answer).lower()
         |        print("cell", c, answer)
         |""".stripMargin

    val script = Files.createTempFile("typecast-double-nan-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))

    val process =
      new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") { process.exitValue() shouldBe 0 }

    val fromScript = cellsOf(out)
    val fromEngine = doubleTargets.zip(columns).flatMap {
      case (to, c) => doubleCases.map(v => s"$c ${engineAnswer(v, to)}")
    }
    withClue(s"script said $fromScript\n") { fromScript shouldBe fromEngine }
    // The stored NaN in each column, which the script used to empty.
    doubleTargets.map(to => engineAnswer(Double.NaN, to)) shouldBe Seq("NaN", "0", "0", "true")
  }

  // Text read as a double and then written back as text, by two operators. A
  // NaN read here has to stay apart from an empty cell for the second cast to
  // write it as "NaN".
  private val doubleTextCases = Seq("NaN", null, "1.25", "Infinity")

  it should "keep a NaN read from text apart from an empty cell" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val toDouble = new TypeCastingOpDesc
    toDouble.typeCastingUnits = List(castUnit("v", AttributeType.DOUBLE))
    val toText = new TypeCastingOpDesc
    toText.typeCastingUnits = List(castUnit("v", AttributeType.STRING))
    val port = toText.operatorInfo.inputPorts.head.id
    val first = toDouble.generateStandaloneCode(
      Map(port -> Schema().add(new Attribute("v", AttributeType.STRING)))
    )
    val second = toText.generateStandaloneCode(
      Map(port -> Schema().add(new Attribute("v", AttributeType.DOUBLE)))
    )

    val values = doubleTextCases
      .map(v => if (v == null) "None" else "\"" + v + "\"")
      .mkString("[", ", ", "]")
    val driver =
      s"""import pandas as pd
         |
         |${toDouble.standaloneHelpers().mkString("\n\n")}
         |
         |
         |in1df = pd.DataFrame({"v": $values})
         |$first
         |in1df = out1df
         |$second
         |
         |for answer in out1df["v"]:
         |    print("cell", "null" if answer is None else answer)
         |""".stripMargin

    val script = Files.createTempFile("typecast-double-text-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))

    val process =
      new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") { process.exitValue() shouldBe 0 }

    val fromEngine = doubleTextCases.map { v =>
      engineAnswer(AttributeTypeUtils.parseField(v, AttributeType.DOUBLE), AttributeType.STRING)
    }
    withClue(s"script said ${cellsOf(out)}\n") { cellsOf(out) shouldBe fromEngine }
    fromEngine shouldBe Seq("NaN", "null", "1.25", "Infinity")
  }

  // Moments either side of 1970, read three ways: as epoch milliseconds, as
  // text that states an offset, and from a column that states its zone. Each
  // asks the machine's zone for the wall clock. Windows' localtime refuses an
  // instant before 1970, which no other platform does, so the driver refuses
  // it the same way here. The engine writes such an instant at the zone's
  // standard offset, as java.util.Date does before 1900.
  private val epochYears = Seq(1600, 2024, 2500)

  it should "read a moment before 1970 where localtime refuses it" taggedAs NeedsPythonPackages in {
    readMomentsAsTheEngineDoes(
      epochYears.map(y =>
        java.time.ZonedDateTime.of(y, 6, 1, 0, 0, 0, 0, java.time.ZoneOffset.UTC)
      ),
      zone = None
    )
  }

  // A zone whose offset has moved, read with the offset in force at each instant.
  // Mexico City was seven hours behind in January 1931, kept summer time until
  // 2022, and before 1922 kept its local mean time; before 1900 java.util.Date
  // takes today's standard offset instead. Only today's two offsets would put
  // the first three an hour or more away.
  private val mexicoCityInstants =
    Seq(
      "1931-01-01T00:00:00Z",
      "2021-07-01T00:00:00Z",
      "1921-01-01T00:00:00Z",
      "1899-12-31T00:00:00Z"
    )
      .map(java.time.ZonedDateTime.parse(_))

  it should "read a moment with the offset its zone had then" taggedAs NeedsPythonPackages in {
    val fromEngine =
      readMomentsAsTheEngineDoes(mexicoCityInstants, zone = Some("America/Mexico_City"))
    // What the pair has to agree on: each offset the zone had, not today's.
    fromEngine.map(moment) shouldBe Seq(
      "1930-12-31 17:00:00",
      "2021-06-30 19:00:00",
      "1920-12-31 17:23:24",
      "1899-12-30 18:00:00"
    ).map(moment)
  }

  /** Casts each instant three ways, as epoch milliseconds, as text that states an
    * offset and from a column that states its zone, with localtime refusing
    * every instant before 1970 as Windows' does, and compares the wall clocks
    * with the engine's. A given zone is set for both sides; otherwise each uses
    * the machine's.
    */
  private def readMomentsAsTheEngineDoes(
      instants: Seq[java.time.ZonedDateTime],
      zone: Option[String]
  ): Seq[String] = {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val op = new TypeCastingOpDesc
    op.typeCastingUnits = List(
      castUnit("ms", AttributeType.TIMESTAMP),
      castUnit("text", AttributeType.TIMESTAMP),
      castUnit("zoned", AttributeType.TIMESTAMP)
    )
    val input = Schema()
      .add(new Attribute("ms", AttributeType.LONG))
      .add(new Attribute("text", AttributeType.STRING))
      .add(new Attribute("zoned", AttributeType.TIMESTAMP))
    val generated = op.generateStandaloneCode(Map(op.operatorInfo.inputPorts.head.id -> input))

    val millis = instants.map(_.toInstant.toEpochMilli)
    val texts = instants.map(_.toLocalDateTime.plusMinutes(330).toString + ":00+05:30")
    val driver =
      s"""import time
         |
         |import numpy as np
         |import pandas as pd
         |
         |${op.standaloneHelpers().mkString("\n\n")}
         |
         |
         |_localtime = time.localtime
         |
         |
         |def _windows_localtime(seconds=None):
         |    if seconds is not None and seconds < 0:
         |        raise OSError(22, "Invalid argument")
         |    return _localtime() if seconds is None else _localtime(seconds)
         |
         |
         |time.localtime = _windows_localtime
         |_utc = ${instants.map(i => "\"" + i.toLocalDateTime + "\"").mkString("[", ", ", "]")}
         |in1df = pd.DataFrame({
         |    "ms": pd.array(${millis.mkString("[", ", ", "]")} + [None], dtype="Int64"),
         |    "text": ${texts.map("\"" + _ + "\"").mkString("[", ", ", "]")} + [None],
         |    "zoned": pd.Series(np.array(_utc + ["NaT"], dtype="datetime64[us]")).dt.tz_localize("UTC"),
         |})
         |$generated
         |
         |for c in ["ms", "text", "zoned"]:
         |    for answer in out1df[c]:
         |        print("cell", c, "null" if pd.isna(answer) else str(answer))
         |""".stripMargin

    val script = Files.createTempFile("typecast-before-1970-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))

    val builder = new ProcessBuilder(python, script.toString).redirectErrorStream(true)
    zone.foreach(z => builder.environment().put("TZ", z))
    val process = builder.start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") { process.exitValue() shouldBe 0 }

    // Every column names the same instants, so each has the engine's answer
    // for the milliseconds. The text is also asked directly.
    val machineZone = java.util.TimeZone.getDefault
    zone.foreach(z => java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone(z)))
    val (fromMillis, fromText) =
      try {
        (
          millis.map(m =>
            engineAnswer(java.lang.Long.valueOf(m), AttributeType.TIMESTAMP)
          ) :+ "null",
          texts.map(t => engineAnswer(t, AttributeType.TIMESTAMP))
        )
      } finally java.util.TimeZone.setDefault(machineZone)
    fromText.map(moment) shouldBe fromMillis.init.map(moment)
    val fromScript = cellsOf(out).map(_.split(" ", 2)).map(p => (p(0), moment(p(1))))
    withClue(s"script said ${cellsOf(out)}\n") {
      fromScript shouldBe Seq("ms", "text", "zoned").flatMap(c =>
        fromMillis.map(v => (c, moment(v)))
      )
    }
    fromMillis.init
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
  // so the pair says the same thing wherever the suite runs. Two carry digits
  // past the millisecond, which DateParserUtils reads into a java.util.Date and
  // so drops.
  private val timestampCases = Seq(
    "2024-03-05 14:09:07",
    "2024-03-05T14:09:07",
    "March 5, 2024",
    "2024-03-05T14:09:07Z",
    "2024-03-05T14:09:07+05:30",
    "2024-03-05 14:09:07.123456",
    "2024-03-05 14:09:07.9999",
    "1677-09-22 00:12:44",
    "2262-04-11 23:47:16",
    "2500-01-01 00:00:00",
    "1500-06-15 08:30:00",
    "9999-12-31 23:59:59"
  )

  /** A printed moment as the wall clock it names. `java.sql.Timestamp.toString`
    * trims its fraction and writes ".0" for none, where Python pads it to six
    * digits or leaves it out.
    */
  private def moment(text: String): Any =
    Try(java.time.LocalDateTime.parse(text.replace(' ', 'T'))).getOrElse(text)

  /** The cells the driver printed, told apart from anything pandas wrote to
    * stderr, which this process merges into the same stream.
    */
  private def cellsOf(out: String): Seq[String] =
    out.linesIterator.filter(_.startsWith("cell ")).map(_.drop("cell ".length)).toSeq

  it should "cast text to a timestamp the way AttributeTypeUtils does" taggedAs NeedsPythonPackages in {
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

    // Only the printed cells, so nothing else written to the merged stderr is
    // read as one.
    val fromScript = cellsOf(out)
    val fromEngine = timestampCases.map(v => engineAnswer(v, AttributeType.TIMESTAMP))
    withClue(s"cases=${timestampCases.mkString(", ")}\nscript said $fromScript\n") {
      fromScript.map(moment) shouldBe fromEngine.map(moment)
    }
    // The rows that made the issue: a moment either side of the nanosecond edge.
    fromEngine.takeRight(3).map(moment) shouldBe
      Seq("2500-01-01 00:00:00", "1500-06-15 08:30:00", "9999-12-31 23:59:59").map(moment)
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

  it should "keep the resolution a timestamp column arrived in" taggedAs NeedsPythonPackages in {
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
  it should "leave a cell it cannot read empty rather than refusing it" taggedAs NeedsPythonPackages in {
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
