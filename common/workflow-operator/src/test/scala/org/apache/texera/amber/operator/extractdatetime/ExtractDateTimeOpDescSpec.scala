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

package org.apache.texera.amber.operator.extractdatetime

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.Timestamp
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class ExtractDateTimeOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private val inputSchema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("ts", AttributeType.TIMESTAMP)
  )

  private def desc(fields: DateTimeField*): ExtractDateTimeOpDesc = {
    val d = new ExtractDateTimeOpDesc
    d.attribute = "ts"
    d.fields = fields.toList
    d
  }

  private def outputSchema(d: ExtractDateTimeOpDesc): Schema =
    d.getPhysicalOp(workflowId, executionId)
      .propagateSchema
      .func(Map(PortIdentity() -> inputSchema))(PortIdentity())

  private def rowsOf(d: ExtractDateTimeOpDesc, moments: Option[String]*): Seq[Seq[Any]] = {
    val exec = new ExtractDateTimeOpExec(objectMapper.writeValueAsString(d))
    exec.open()
    val out = moments.zipWithIndex.map {
      case (moment, i) =>
        val b = Tuple.builder(inputSchema)
        b.add(inputSchema.getAttribute("id"), Int.box(i))
        b.add(inputSchema.getAttribute("ts"), moment.map(Timestamp.valueOf).orNull)
        exec.processTuple(b.build(), 0).next().getFields.toSeq
    }
    exec.close()
    out
  }

  "ExtractDateTimeOpDesc.operatorInfo" should "advertise the name and the Cleaning group" in {
    val info = (new ExtractDateTimeOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Extract Date/Time Fields"
    info.operatorGroupName shouldBe OperatorGroupConstants.CLEANING_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "ExtractDateTimeOpDesc.getPhysicalOp" should "wire ExtractDateTimeOpExec" in {
    (new ExtractDateTimeOpDesc)
      .getPhysicalOp(workflowId, executionId)
      .opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.extractdatetime.ExtractDateTimeOpExec"
      case other => fail(s"unexpected executor: $other")
    }
  }

  "The output schema" should "name each added column after the source and the field" in {
    val schema = outputSchema(desc(DateTimeField.YEAR, DateTimeField.DAY_OF_WEEK))
    schema.getAttributeNames shouldBe List("id", "ts", "ts_year", "ts_day_of_week")
    schema.getAttribute("ts_year").getType shouldBe AttributeType.INTEGER
    schema.getAttribute("ts_day_of_week").getType shouldBe AttributeType.INTEGER
  }

  it should "keep the input untouched when no field is asked for" in {
    outputSchema(desc()).getAttributeNames shouldBe List("id", "ts")
  }

  // The same field twice names one column, so the repeat is dropped rather than
  // reaching the schema as a duplicate.
  it should "add one column for a field asked for twice" in {
    outputSchema(desc(DateTimeField.YEAR, DateTimeField.YEAR)).getAttributeNames shouldBe List(
      "id",
      "ts",
      "ts_year"
    )
  }

  it should "refuse a derived name the input already carries" in {
    val d = new ExtractDateTimeOpDesc
    d.attribute = "ts"
    d.fields = List(DateTimeField.YEAR)
    val clashing = new Schema(
      new Attribute("ts", AttributeType.TIMESTAMP),
      new Attribute("ts_year", AttributeType.INTEGER)
    )
    a[RuntimeException] should be thrownBy
      d.getPhysicalOp(workflowId, executionId)
        .propagateSchema
        .func(Map(PortIdentity() -> clashing))
  }

  // 2024-03-05 14:09:07 is a Tuesday in ISO week 10 of Q1, day 65 of the year.
  "The executor" should "read every field the way ISO-8601 states it" in {
    val d = desc(
      DateTimeField.YEAR,
      DateTimeField.QUARTER,
      DateTimeField.MONTH,
      DateTimeField.DAY,
      DateTimeField.DAY_OF_WEEK,
      DateTimeField.DAY_OF_YEAR,
      DateTimeField.WEEK_OF_YEAR,
      DateTimeField.HOUR,
      DateTimeField.MINUTE,
      DateTimeField.SECOND
    )
    rowsOf(d, Some("2024-03-05 14:09:07")).head.drop(2) shouldBe
      Seq(2024, 1, 3, 5, 2, 65, 10, 14, 9, 7)
  }

  it should "count Monday as 1 and Sunday as 7" in {
    val week = Seq(
      "2024-03-04",
      "2024-03-05",
      "2024-03-06",
      "2024-03-07",
      "2024-03-08",
      "2024-03-09",
      "2024-03-10"
    ).map(day => Some(s"$day 00:00:00"))
    rowsOf(desc(DateTimeField.DAY_OF_WEEK), week: _*).map(_.last) shouldBe Seq(1, 2, 3, 4, 5, 6, 7)
  }

  // The operator adds columns; it says nothing about which rows belong, so a row
  // whose timestamp is empty keeps its place with the added columns empty.
  it should "leave the added columns empty for an empty timestamp, and keep the row" in {
    val rows = rowsOf(
      desc(DateTimeField.YEAR, DateTimeField.MONTH),
      Some("2024-03-05 14:09:07"),
      None
    )
    rows should have length 2
    rows(1).drop(2) shouldBe Seq(null, null)
  }

  "The generated Python" should "state the ISO weekday, which pandas does not" in {
    desc(DateTimeField.DAY_OF_WEEK).generateStandaloneCode() should
      include("_texera_ts.dt.dayofweek + 1")
  }

  it should "hold the source and the derived name as escaped literals" in {
    val d = new ExtractDateTimeOpDesc
    d.attribute = "a\"b"
    d.fields = List(DateTimeField.YEAR)
    val code = d.generateStandaloneCode()
    code should include("""out1df["a\"b"]""")
    code should include("""out1df["a\"b_year"]""")
  }

  it should "copy the frame through when no field is asked for" in {
    desc().generateStandaloneCode() shouldBe "out1df = in1df.copy()"
  }

  // pandas parses into nanoseconds by default, and nanoseconds reach only 1677 to
  // 2262. The engine reads a java.sql.Timestamp, which holds the year 2500 like any
  // other, so parsing the column the default way would end the exported run on a
  // moment the operator itself has no trouble with.
  it should "parse the column at a resolution that reaches past the year 2262" in {
    desc(DateTimeField.YEAR).generateStandaloneCode() should
      include("""astype("datetime64[us]")""")
  }

  private val allFields = Seq(
    DateTimeField.YEAR,
    DateTimeField.QUARTER,
    DateTimeField.MONTH,
    DateTimeField.DAY,
    DateTimeField.DAY_OF_WEEK,
    DateTimeField.DAY_OF_YEAR,
    DateTimeField.WEEK_OF_YEAR,
    DateTimeField.HOUR,
    DateTimeField.MINUTE,
    DateTimeField.SECOND
  )

  // The moments either side of the nanosecond edge, and far past it in both
  // directions, read against the executor's own answer for the same moment.
  it should "read the same fields as the executor at and past the nanosecond edge" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImport(python, "pandas")) cancel(s"'$python' cannot import pandas")

    val moments = Seq(
      "1677-09-22 00:12:44",
      "2262-04-11 23:47:16",
      "2500-01-01 14:09:07",
      "1500-06-15 08:30:00",
      "9999-12-31 23:59:59"
    )
    val d = desc(allFields: _*)
    val expected = rowsOf(d, moments.map(Some(_)): _*).map(_.drop(2).mkString(","))

    val driver =
      s"""import pandas as pd
         |
         |in1df = pd.DataFrame({"ts": [${moments.map(m => s""""$m"""").mkString(", ")}]})
         |${d.generateStandaloneCode()}
         |
         |for _row in out1df.drop(columns=["ts"]).itertuples(index=False):
         |    print(",".join(str(_v) for _v in _row))
         |""".stripMargin

    val script = Files.createTempFile("extract-datetime-bounds-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(180, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      out.trim.linesIterator.toSeq shouldBe expected
    }
  }

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

  private def canImport(python: String, modules: String): Boolean =
    Try(
      new ProcessBuilder(python, "-c", s"import $modules").redirectErrorStream(true).start()
    ).toOption.exists { p =>
      if (!p.waitFor(120, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
}
