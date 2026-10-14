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

package org.apache.texera.amber.operator.aggregate

import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorMetadataGenerator}
import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.operator.tags.IntegrationTest
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.Tag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Try

class AggregateOpDescSpec extends AnyFlatSpec with Matchers {

  private val NeedsPythonPackages = Tag(classOf[IntegrationTest].getName)

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def aggOp(fn: AggregationFunction, attr: String, result: String): AggregationOperation = {
    val a = new AggregationOperation()
    a.aggFunction = fn
    a.attribute = attr
    a.resultAttribute = result
    a
  }

  // Each test builds a FRESH desc: getPhysicalPlan mutates `aggregations` (getFinal),
  // so the descriptor is intentionally not idempotent across calls.
  private def descWith(keys: List[String], aggs: AggregationOperation*): AggregateOpDesc = {
    val d = new AggregateOpDesc
    d.groupByKeys = keys
    d.aggregations = aggs.toList
    d
  }

  "AggregateOpDesc.operatorInfo" should "advertise the name and Aggregate group" in {
    val info = (new AggregateOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Aggregate"
    info.operatorDescription shouldBe "Calculate different types of aggregation values"
    info.operatorGroupName shouldBe OperatorGroupConstants.AGGREGATE_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe false
  }

  "AggregateOpDesc.getPhysicalPlan" should
    "build a two-stage (localAgg + globalAgg) plan with one connecting link" in {
    val plan = descWith(List("city"), aggOp(AggregationFunction.SUM, "sales", "total"))
      .getPhysicalPlan(workflowId, executionId)
    plan.operators should have size 2
    plan.links should have size 1
  }

  "AggregateOpDesc schema propagation" should
    "produce the group-by keys plus the aggregation result column (SUM keeps the input type)" in {
    val input = Schema().add("city", AttributeType.STRING).add("sales", AttributeType.INTEGER)
    val out = descWith(List("city"), aggOp(AggregationFunction.SUM, "sales", "total"))
      .getExternalOutputSchemas(Map(PortIdentity() -> input))
    out shouldBe Map(
      PortIdentity() -> Schema()
        .add("city", AttributeType.STRING)
        .add("total", AttributeType.INTEGER)
    )
  }

  it should "type a COUNT result as INTEGER and an AVERAGE result as DOUBLE" in {
    val input = Schema().add("v", AttributeType.LONG)
    descWith(List.empty, aggOp(AggregationFunction.COUNT, "v", "cnt"))
      .getExternalOutputSchemas(Map(PortIdentity() -> input)) shouldBe
      Map(PortIdentity() -> Schema().add("cnt", AttributeType.INTEGER))
    descWith(List.empty, aggOp(AggregationFunction.AVERAGE, "v", "avg"))
      .getExternalOutputSchemas(Map(PortIdentity() -> input)) shouldBe
      Map(PortIdentity() -> Schema().add("avg", AttributeType.DOUBLE))
  }

  it should "type a COUNT(*) (empty attribute) result as INTEGER without looking up an input column" in {
    // An empty attribute means COUNT(*); schema propagation must not dereference a column.
    val input = Schema().add("v", AttributeType.LONG)
    descWith(List.empty, aggOp(AggregationFunction.COUNT, "", "row_count"))
      .getExternalOutputSchemas(Map(PortIdentity() -> input)) shouldBe
      Map(PortIdentity() -> Schema().add("row_count", AttributeType.INTEGER))
  }

  it should "fail fast for a non-COUNT function with an empty attribute (only COUNT allows it)" in {
    // Only COUNT tolerates a blank attribute; SUM/etc. must resolve the column and fail
    // fast rather than propagate a null-typed output.
    val input = Schema().add("v", AttributeType.LONG)
    assertThrows[Exception] {
      descWith(List.empty, aggOp(AggregationFunction.SUM, "", "total"))
        .getExternalOutputSchemas(Map(PortIdentity() -> input))
    }
  }

  // The integers behind a timestamp column mean microseconds or nanoseconds
  // depending on the resolution it was read at, and a nanosecond total leaves
  // the range of a 64-bit integer after a handful of modern dates.
  it should "read a timestamp as epoch milliseconds at either resolution" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val input = Schema().add("t", AttributeType.TIMESTAMP)
    val desc = descWith(
      List.empty,
      aggOp(AggregationFunction.SUM, "t", "ts_total"),
      aggOp(AggregationFunction.AVERAGE, "t", "ts_avg")
    )
    val block = desc.generateStandaloneCode(Map(PortIdentity() -> input))

    def report(unit: String): String =
      s"""in1df = pd.DataFrame({
         |    "t": pd.to_datetime(["2024-01-01"] * 6).astype("datetime64[$unit]"),
         |})
         |$block
         |print(pd.Timestamp(out1df.iloc[0]["ts_total"]).isoformat())
         |print(repr(float(out1df.iloc[0]["ts_avg"])))""".stripMargin

    val driver =
      s"""import pandas as pd
         |${desc.standaloneHelpers().mkString("\n")}
         |${report("ns")}
         |${report("us")}
         |""".stripMargin

    val out = runPython(python, driver, "aggregate-timestamp-units-", "UTC")

    withClue(s"python said:\n$out\nscript:\n$driver") {
      // Six times 1704067200000 milliseconds, read back as a timestamp: a date
      // no nanosecond count can hold, and the same one at either resolution.
      out.trim.linesIterator.toSeq shouldBe Seq(
        "2293-12-31T00:00:00",
        "1704067200000.0",
        "2293-12-31T00:00:00",
        "1704067200000.0"
      )
    }
  }

  /** `Timestamp.getTime` answers for the instant a wall clock names in the
    * JVM's default zone, and `new Timestamp(long)` renders one back there, so a
    * script that read the column as UTC was a whole offset out on every value
    * it added. Checked against Java 17 in the same zone: the sum below is
    * 2078-01-01 06:00:00 and the average 1704132000000.
    */
  it should "add timestamps in the zone the script runs in" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val input = Schema().add("t", AttributeType.TIMESTAMP)
    val desc = descWith(
      List.empty,
      aggOp(AggregationFunction.SUM, "t", "ts_total"),
      aggOp(AggregationFunction.AVERAGE, "t", "ts_avg")
    )
    val block = desc.generateStandaloneCode(Map(PortIdentity() -> input))

    val driver =
      s"""import pandas as pd
         |${desc.standaloneHelpers().mkString("\n")}
         |in1df = pd.DataFrame({
         |    "t": pd.to_datetime(["2024-01-01 00:00:00", "2024-01-02 00:00:00"]),
         |})
         |$block
         |print(pd.Timestamp(out1df.iloc[0]["ts_total"]).isoformat())
         |print(repr(float(out1df.iloc[0]["ts_avg"])))
         |""".stripMargin

    // A zone six hours behind UTC and with no daylight saving of its own, so
    // one offset covers both dates.
    val out = runPython(python, driver, "aggregate-timestamp-zone-", "America/Mexico_City")

    withClue(s"python said:\n$out\nscript:\n$driver") {
      out.trim.linesIterator.toSeq shouldBe Seq("2078-01-01T06:00:00", "1704132000000.0")
    }
  }

  /** With no zone named, as on Windows without tzlocal, the script asks
    * tzlocal(), which asks localtime, and Windows refuses an instant before
    * 1970: the column raised and the sum came back in UTC. The driver stands in
    * for both. Checked against Java 17 in the same zone: the sum below is
    * 1653-12-31 18:00:00 and the average -4.9859712E12.
    */
  it should "add timestamps before 1970 where localtime refuses them" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val input = Schema().add("t", AttributeType.TIMESTAMP)
    val desc = descWith(
      List.empty,
      aggOp(AggregationFunction.SUM, "t", "ts_total"),
      aggOp(AggregationFunction.AVERAGE, "t", "ts_avg")
    )
    val block = desc.generateStandaloneCode(Map(PortIdentity() -> input))

    val driver =
      s"""import time
         |import numpy as np
         |import pandas as pd
         |${desc.standaloneHelpers().mkString("\n")}
         |_localtime = time.localtime
         |def _refusing(seconds=None):
         |    if seconds is not None and seconds < 0:
         |        raise OSError(22, "Invalid argument")
         |    return _localtime() if seconds is None else _localtime(seconds)
         |time.localtime = _refusing
         |_texera_local_zone = lambda: None
         |in1df = pd.DataFrame({
         |    "t": np.array(["1600-01-01T12:00", "2024-01-01T00:00"], dtype="datetime64[ms]"),
         |})
         |$block
         |print(pd.Timestamp(out1df.iloc[0]["ts_total"]).isoformat())
         |print(repr(float(out1df.iloc[0]["ts_avg"])))
         |""".stripMargin

    val out = runPython(python, driver, "aggregate-timestamp-refused-", "America/Mexico_City")

    withClue(s"python said:\n$out\nscript:\n$driver") {
      out.trim.linesIterator.toSeq shouldBe Seq("1653-12-31T18:00:00", "-4985971200000.0")
    }
  }

  // A zone whose offset has moved: Mexico City was seven hours behind in 1931,
  // kept summer time until 2022 and its local mean time until 1922, and before
  // 1900 java.util.Date reads it at today's standard offset. One group per row,
  // so each AVERAGE is that wall clock's epoch milliseconds.
  private val mexicoCityWallClocks = Seq(
    "1930-12-31 17:00:00",
    "2021-06-30 19:00:00",
    "1920-12-31 17:23:24",
    "1850-01-01 00:00:00"
  )

  it should "read each timestamp with the offset its zone had then" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val input = Schema().add("k", AttributeType.INTEGER).add("t", AttributeType.TIMESTAMP)
    val desc = descWith(List("k"), aggOp(AggregationFunction.AVERAGE, "t", "ts_avg"))
    val block = desc.generateStandaloneCode(Map(PortIdentity() -> input))

    val values = mexicoCityWallClocks.map("\"" + _ + "\"").mkString("[", ", ", "]")
    val driver =
      s"""import pandas as pd
         |${desc.standaloneHelpers().mkString("\n")}
         |in1df = pd.DataFrame({
         |    "k": list(range(${mexicoCityWallClocks.size})),
         |    "t": pd.to_datetime($values),
         |})
         |$block
         |for avg in out1df.sort_values("k")["ts_avg"]:
         |    print(int(avg))
         |""".stripMargin

    val zone = "America/Mexico_City"
    val out = runPython(python, driver, "aggregate-timestamp-history-", zone)

    val machineZone = java.util.TimeZone.getDefault
    java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone(zone))
    val fromEngine =
      try mexicoCityWallClocks.map(w => java.sql.Timestamp.valueOf(w).getTime.toString)
      finally java.util.TimeZone.setDefault(machineZone)

    withClue(s"python said:\n$out\nscript:\n$driver") {
      out.trim.linesIterator.toSeq shouldBe fromEngine
    }
    fromEngine shouldBe Seq("-1230768000000", "1625097600000", "-1546300800000", "-3786804000000")
  }

  /** Runs the generated block with the zone named to the child, because a
    * timestamp's arithmetic reads the one in force where it runs.
    */
  private def runPython(python: String, driver: String, prefix: String, zone: String): String = {
    val script = Files.createTempFile(prefix, ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val builder = new ProcessBuilder(python, script.toString).redirectErrorStream(true)
    builder.environment().put("TZ", zone)
    val process = builder.start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver")(process.exitValue() shouldBe 0)
    out
  }

  // A group keyed on a missing value and one keyed on a NaN are two groups to
  // the engine. The two are only distinct in a nullable dtype, which is what an
  // Arrow file is read into.
  it should "group a missing key apart from a NaN one" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val desc = descWith(List("k"), aggOp(AggregationFunction.SUM, "v", "total"))

    val driver =
      s"""import pandas as pd, numpy as np
         |
         |in1df = pd.DataFrame({
         |    "k": pd.arrays.FloatingArray(np.array([0.0, np.nan]), np.array([True, False])),
         |    "v": [1, 2],
         |})
         |${desc.generateStandaloneCode()}
         |print(len(out1df), sorted(int(x) for x in out1df["total"]))
         |""".stripMargin

    val script = Files.createTempFile("aggregate-null-nan-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      // Two groups of one row each, not one group totalling 3.
      out.trim shouldBe "2 [1, 2]"
    }
  }

  // The engine keys a group on a List of the key fields in a HashMap, under
  // Scala's ==: NaN equals nothing, so each NaN row is a group of its own,
  // while 0.0 and -0.0 are one group and so are two nulls. Both stages run, as
  // the plan runs them.
  it should "group NaN and signed-zero keys as the engine does" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val count = aggOp(AggregationFunction.COUNT, "v", "n")
    val desc = descWith(List("k"), count)
    val keys: Seq[java.lang.Double] = Seq(Double.NaN, Double.NaN, 0.0, -0.0, null, null, 1.0)

    val input = Schema().add("k", AttributeType.DOUBLE).add("v", AttributeType.INTEGER)
    val local = new AggregateOpExec(objectMapper.writeValueAsString(desc))
    local.open()
    keys.foreach(k => local.processTuple(Tuple(input, Array[Any](k, 1)), 0))
    val partials = local.onFinish(0).toList
    val partialSchema = Schema().add("k", AttributeType.DOUBLE).add("n", AttributeType.INTEGER)
    val global =
      new AggregateOpExec(objectMapper.writeValueAsString(descWith(List("k"), count.getFinal)))
    global.open()
    partials.foreach(t => global.processTuple(Tuple(partialSchema, t.getFields.toArray[Any]), 0))
    val engine = global
      .onFinish(0)
      .map { row =>
        val key = row.getFields.head match {
          case null                           => "None"
          case d: java.lang.Double if d.isNaN => "nan"
          case d                              => d.toString
        }
        s"$key ${row.getFields(1)}"
      }
      .toList
      .sorted

    val pyKeys = keys.map(k => if (k == null) "None" else if (k.isNaN) "np.nan" else k.toString)
    val driver =
      s"""import pandas as pd, numpy as np
         |
         |keys = [${pyKeys.mkString(", ")}]
         |in1df = pd.DataFrame({
         |    "k": pd.arrays.FloatingArray(
         |        np.array([0.0 if k is None else k for k in keys]),
         |        np.array([k is None for k in keys]),
         |    ),
         |    "v": [1] * len(keys),
         |})
         |${desc.generateStandaloneCode()}
         |rows = [
         |    ("None" if k is pd.NA else repr(float(k))) + " " + str(int(n))
         |    for k, n in zip(out1df["k"], out1df["n"])
         |]
         |print("\\n".join(sorted(rows)))
         |""".stripMargin

    val script = Files.createTempFile("aggregate-nan-zero-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      out.trim.linesIterator.toList shouldBe engine
    }
  }

  it should "emit no row for an empty input with no group keys, as the engine does" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val desc = descWith(List.empty, aggOp(AggregationFunction.COUNT, "", "cnt"))

    val exec = new AggregateOpExec(objectMapper.writeValueAsString(desc))
    exec.open()
    exec.onFinish(0).toList shouldBe empty

    val driver =
      s"""import pandas as pd
         |
         |in1df = pd.DataFrame({"v": pd.Series([], dtype="int64")})
         |${desc.generateStandaloneCode()}
         |print(len(out1df), list(out1df.columns))
         |""".stripMargin
    val out = runPython(python, driver, "aggregate-empty-", "UTC")
    withClue(s"script:\n$driver")(out.trim shouldBe "0 ['cnt']")
  }

  // A DOUBLE column read from Arrow keeps a stored NaN apart from null. The
  // engine skips the null and folds the NaN in, so SUM, AVERAGE and MAX answer
  // NaN while MIN, which Java's Double.compare sends NaN past, answers 1.0.
  // CONCAT writes the NaN out and only a null as the empty string.
  it should "fold a stored NaN in a DOUBLE column as the engine does" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val input = Schema().add("k", AttributeType.STRING).add("v", AttributeType.DOUBLE)
    // Group "a" holds a NaN between two numbers and group "b" only nulls.
    val rows: Seq[(String, java.lang.Double)] =
      Seq(("a", 1.0), ("a", Double.NaN), ("a", 3.0), ("b", null), ("b", null))
    val functions =
      Seq(
        AggregationFunction.SUM,
        AggregationFunction.AVERAGE,
        AggregationFunction.MIN,
        AggregationFunction.MAX,
        AggregationFunction.CONCAT
      )

    def render(value: Any): String =
      value match {
        case null                           => "None"
        case d: java.lang.Double if d.isNaN => "nan"
        case d                              => d.toString
      }

    def check(keys: List[String]): Unit = {
      val desc =
        () => descWith(keys, functions.map(fn => aggOp(fn, "v", fn.toString.toLowerCase)): _*)

      val exec = new AggregateOpExec(objectMapper.writeValueAsString(desc()))
      exec.open()
      rows.foreach { case (k, v) => exec.processTuple(Tuple(input, Array[Any](k, v)), 0) }
      val engine = exec
        .onFinish(0)
        .map(_.getFields.toSeq.map(render).mkString(" "))
        .toSeq
        .sorted

      val driver =
        s"""import pandas as pd, numpy as np
           |
           |in1df = pd.DataFrame({
           |    "k": ["a", "a", "a", "b", "b"],
           |    "v": pd.arrays.FloatingArray(
           |        np.array([1.0, np.nan, 3.0, 0.0, 0.0]),
           |        np.array([False, False, False, True, True]),
           |    ),
           |})
           |${desc().standaloneHelpers().mkString("\n")}
           |${desc().generateStandaloneCode(Map(PortIdentity() -> input))}
           |def render(v):
           |    if v is pd.NA or v is None:
           |        return "None"
           |    return v if isinstance(v, str) else repr(float(v))
           |for row in sorted(" ".join(render(v) for v in r) for r in out1df.itertuples(index=False)):
           |    print(row)
           |""".stripMargin
      val out = runPython(python, driver, "aggregate-double-nan-", "UTC")
      // No trim: an all-null group's CONCAT is "", which ends its line in a space.
      withClue(s"script:\n$driver")(out.linesIterator.toSeq shouldBe engine)
    }

    check(List("k"))
    check(List.empty)
  }

  // Without a schema the type-led branches cannot be chosen.
  it should "fall back to pandas' own answers when no schema is given" in {
    val desc = descWith(List.empty, aggOp(AggregationFunction.SUM, "i", "int_total"))
    desc.generateStandaloneCode() should include("in1df[\"i\"].sum()")
    desc.generateStandaloneCode() should not include "_texera_agg_int_sum(in1df"
  }

  "AggregateOpDesc JSON schema" should
    "make the attribute optional only for count and required for every other function" in {
    val aggDef = OperatorMetadataGenerator
      .generateOperatorJsonSchema(classOf[AggregateOpDesc])
      .get("definitions")
      .get("AggregationOperation")

    // attribute is not unconditionally required (aggFunction still is)
    val baseRequired = aggDef.get("required").elements().asScala.map(_.asText()).toSet
    baseRequired should contain("aggFunction")
    baseRequired should not contain "attribute"

    // conditional rule: count -> no attribute requirement; any other function -> attribute required
    val rule = aggDef
      .get("allOf")
      .elements()
      .asScala
      .find(node => node.has("if") && node.has("else"))
      .getOrElse(fail("expected a conditional if/else rule in the AggregationOperation schema"))
    rule.get("if").get("properties").get("aggFunction").get("const").asText() shouldBe "count"
    val elseRequired = rule.get("else").get("required").elements().asScala.map(_.asText()).toList
    elseRequired should contain("attribute")
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

  private def canImportPandas(python: String): Boolean =
    Try(
      new ProcessBuilder(python, "-c", "import pandas, numpy").redirectErrorStream(true).start()
    ).toOption.exists { p =>
      if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
}
