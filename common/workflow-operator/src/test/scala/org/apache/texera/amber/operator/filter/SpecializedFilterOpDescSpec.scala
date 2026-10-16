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

package org.apache.texera.amber.operator.filter

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.tags.IntegrationTest
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.Tag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.sql.Timestamp
import java.time.{LocalDateTime, ZoneOffset}
import java.util.TimeZone
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class SpecializedFilterOpDescSpec extends AnyFlatSpec with Matchers {

  private val NeedsPythonPackages = Tag(classOf[IntegrationTest].getName)

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "SpecializedFilterOpDesc.operatorInfo" should
    "advertise the name, Cleaning group, and reconfiguration support" in {
    val info = (new SpecializedFilterOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Filter"
    info.operatorGroupName shouldBe OperatorGroupConstants.CLEANING_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "SpecializedFilterOpDesc.predicates" should "default to an empty list" in {
    (new SpecializedFilterOpDesc).predicates shouldBe empty
  }

  "SpecializedFilterOpDesc.getPhysicalOp" should
    "wire SpecializedFilterOpExec and carry port identities" in {
    val op = new SpecializedFilterOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.filter.SpecializedFilterOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "SpecializedFilterOpDesc" should
    "round-trip (default empty predicates) through the polymorphic base" in {
    val restored =
      objectMapper.readValue(
        objectMapper.writeValueAsString(new SpecializedFilterOpDesc),
        classOf[LogicalOp]
      )
    restored shouldBe a[SpecializedFilterOpDesc]
    restored.asInstanceOf[SpecializedFilterOpDesc].predicates shouldBe empty
  }

  "SpecializedFilterOpDesc.generateStandaloneCode" should "emit one condition per predicate" in {
    val d = new SpecializedFilterOpDesc
    d.predicates = List(new FilterPredicate("age", ComparisonType.GREATER_THAN, "18"))
    val code = d.generateStandaloneCode()
    code should include("in1df[\"age\"]")
    code should include("out1df")
  }

  // The executor filters on `predicates.exists`, which answers false on an empty
  // list, so no predicate keeps no row. The columns survive; the rows do not.
  it should "keep no row when there is no predicate" in {
    (new SpecializedFilterOpDesc).generateStandaloneCode() shouldBe
      "out1df = in1df.iloc[0:0].copy()"
  }

  /** One column of a given type, the rows the engine reads and the frame the
    * script reads, and the predicates run over it.
    */
  private case class Column(
      attrType: AttributeType,
      values: Seq[Any],
      pandas: String,
      predicates: Seq[(ComparisonType, String)]
  )

  // A column holds a wall clock, which the engine reads in the JVM's zone and
  // pandas holds without one, so both sides are built from the same wall clock.
  private def timestamp(micros: Long): Timestamp =
    Timestamp.valueOf(
      LocalDateTime.ofEpochSecond(
        Math.floorDiv(micros, 1000000L),
        (Math.floorMod(micros, 1000000L) * 1000).toInt,
        ZoneOffset.UTC
      )
    )

  // 2024-01-01 00:00:00 plus a few microseconds, and a millisecond later.
  private val midnight = 1704067200000000L

  private val columns = Seq(
    Column(
      AttributeType.DOUBLE,
      Seq(Double.NaN, -0.0, 0.0, 1.0, null, Double.PositiveInfinity),
      """pd.arrays.FloatingArray(
        |    np.array([np.nan, -0.0, 0.0, 1.0, 0.0, np.inf]),
        |    np.array([False, False, False, False, True, False]),
        |)""".stripMargin,
      Seq(
        ComparisonType.EQUAL_TO -> "0",
        ComparisonType.GREATER_THAN -> "0",
        ComparisonType.GREATER_THAN_OR_EQUAL_TO -> "0",
        ComparisonType.LESS_THAN -> "0",
        ComparisonType.LESS_THAN_OR_EQUAL_TO -> "0",
        ComparisonType.NOT_EQUAL_TO -> "0",
        ComparisonType.GREATER_THAN -> "1",
        ComparisonType.EQUAL_TO -> "NaN",
        ComparisonType.LESS_THAN -> "Infinity",
        ComparisonType.EQUAL_TO -> "-0.0",
        // Hex past the largest double rounds to infinity, and below the
        // smallest to a zero that keeps its sign.
        ComparisonType.EQUAL_TO -> "0x1.fffffffffffff8p1023",
        ComparisonType.EQUAL_TO -> "-0x1p-1080"
      )
    ),
    Column(
      AttributeType.DOUBLE,
      Seq(Double.PositiveInfinity, 1.0, null, Double.NegativeInfinity),
      """pd.arrays.FloatingArray(
        |    np.array([np.inf, 1.0, 0.0, -np.inf]),
        |    np.array([False, False, True, False]),
        |)""".stripMargin,
      Seq(
        ComparisonType.EQUAL_TO -> "0x1p1024",
        ComparisonType.EQUAL_TO -> "-0x1p1024",
        ComparisonType.LESS_THAN -> "0x1p1024d",
        ComparisonType.EQUAL_TO -> "0x1p0"
      )
    ),
    Column(
      AttributeType.STRING,
      Seq("1", "2", "10", "abc", null, " 3 ", "nan", "2d", "0x1p2000"),
      """pd.Series(["1", "2", "10", "abc", None, " 3 ", "nan", "2d", "0x1p2000"], dtype=object)""",
      Seq(
        ComparisonType.EQUAL_TO -> "Infinity",
        ComparisonType.EQUAL_TO -> "1",
        ComparisonType.GREATER_THAN -> "2",
        ComparisonType.EQUAL_TO -> "1.0",
        ComparisonType.LESS_THAN -> "b",
        ComparisonType.EQUAL_TO -> "nan",
        ComparisonType.EQUAL_TO -> "2",
        ComparisonType.NOT_EQUAL_TO -> "abc"
      )
    ),
    Column(
      AttributeType.BOOLEAN,
      Seq(true, false, null),
      """pd.array([True, False, None], dtype="boolean")""",
      Seq(
        ComparisonType.EQUAL_TO -> "True",
        ComparisonType.EQUAL_TO -> "1",
        ComparisonType.GREATER_THAN -> "false"
      )
    ),
    Column(
      AttributeType.INTEGER,
      Seq(1, 2, null, -3),
      """pd.array([1, 2, None, -3], dtype="Int32")""",
      Seq(ComparisonType.GREATER_THAN -> "1.5", ComparisonType.EQUAL_TO -> "2")
    ),
    Column(
      AttributeType.LONG,
      Seq(1L, 9007199254740993L, null),
      """pd.array([1, 9007199254740993, None], dtype="Int64")""",
      Seq(
        ComparisonType.EQUAL_TO -> "9007199254740993",
        ComparisonType.GREATER_THAN -> " 1 "
      )
    ),
    Column(
      AttributeType.TIMESTAMP,
      Seq(midnight, midnight + 900, midnight + 1000).map(timestamp),
      s"""pd.Series(np.array([$midnight, ${midnight + 900}, ${midnight + 1000}], dtype="datetime64[us]"))""",
      Seq(
        ComparisonType.EQUAL_TO -> "2024-01-01 00:00:00",
        ComparisonType.GREATER_THAN -> "2024-01-01 00:00:00"
      )
    )
  )

  // FilterPredicate compares by the column's type: a STRING field and the value
  // as numbers when both parse as one, a BOOLEAN as its lower-case text, an
  // INTEGER as a double, a DOUBLE by Double.compareTo, and a TIMESTAMP by
  // getTime. Each predicate runs through SpecializedFilterOpExec and through
  // the export, and the rows each keeps are named by their input position.
  it should "keep the rows the engine keeps for each column type" taggedAs NeedsPythonPackages in {
    expectTheRowsTheEngineKeeps(columns)
  }

  // The engine holds a value with an offset as the wall clock it shows in the
  // JVM's zone. A zone away from UTC, and a half hour off any whole hour, so a
  // reading in UTC or a dropped offset keeps a different row.
  it should "read a timestamp value with an offset in the machine's zone" taggedAs
    NeedsPythonPackages in {
    val zone = "Asia/Kolkata"
    val previous = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone(zone))
    try {
      val utcMidnight = midnight + 19800000000L
      expectTheRowsTheEngineKeeps(
        Seq(
          Column(
            AttributeType.TIMESTAMP,
            Seq(timestamp(utcMidnight), timestamp(midnight), null),
            s"""pd.Series(np.array([$utcMidnight, $midnight, "NaT"], dtype="datetime64[us]"))""",
            Seq(
              ComparisonType.EQUAL_TO -> "2024-01-01T00:00:00Z",
              ComparisonType.EQUAL_TO -> "2024-01-01 00:00:00",
              ComparisonType.EQUAL_TO -> "2024-01-01T00:00:00+05:30",
              ComparisonType.EQUAL_TO -> "2023-12-31T16:00:00-08:00",
              ComparisonType.GREATER_THAN -> "2024-01-01T00:00:00Z",
              ComparisonType.LESS_THAN -> "2024-01-01T00:00:00Z"
            )
          )
        ),
        pythonZone = Some(zone)
      )
    } finally TimeZone.setDefault(previous)
  }

  /** Runs each predicate through SpecializedFilterOpExec and through the export,
    * and expects the export to keep the same rows, named by input position.
    */
  private def expectTheRowsTheEngineKeeps(
      columns: Seq[Column],
      pythonZone: Option[String] = None
  ): Unit = {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val runs = for {
      column <- columns
      (condition, value) <- column.predicates
    } yield {
      val schema = Schema().add("v", column.attrType).add("i", AttributeType.INTEGER)
      val desc = new SpecializedFilterOpDesc
      desc.predicates = List(new FilterPredicate("v", condition, value))
      val exec = new SpecializedFilterOpExec(objectMapper.writeValueAsString(desc))
      exec.open()
      val kept = column.values.zipWithIndex.flatMap {
        case (v, i) => exec.processTuple(Tuple(schema, Array[Any](v, i)), 0)
      }
      exec.close()
      val label = s"${column.attrType} ${condition.getName} '$value'"
      val engine = s"$label: ${kept.map(_.getFields(1)).mkString(" ")}"
      val code = desc.generateStandaloneCode(Map(PortIdentity() -> schema))
      val script =
        s"""in1df = pd.DataFrame({"v": ${column.pandas.linesIterator.mkString("\n    ")}})
           |in1df["i"] = range(len(in1df))
           |ns = {"pd": pd, "in1df": in1df}
           |exec(${objectMapper.writeValueAsString(
          (desc.standaloneHelpers() :+ code).mkString("\n")
        )}, ns)
           |print(${objectMapper.writeValueAsString(
          label + ":"
        )}, " ".join(str(i) for i in ns["out1df"]["i"]))""".stripMargin
      (engine, script)
    }

    val driver = ("import pandas as pd, numpy as np" +: runs.map(_._2)).mkString("\n")
    val file = Files.createTempFile("filter-engine-parity-", ".py")
    file.toFile.deleteOnExit()
    Files.write(file, driver.getBytes(StandardCharsets.UTF_8))
    val builder = new ProcessBuilder(python, file.toString).redirectErrorStream(true)
    pythonZone.foreach(builder.environment().put("TZ", _))
    val process = builder.start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)

    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      out.linesIterator.map(_.trim).toList shouldBe runs.map(_._1.trim).toList
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

  private def canImportPandas(python: String): Boolean =
    Try(
      new ProcessBuilder(python, "-c", "import pandas, numpy").redirectErrorStream(true).start()
    ).toOption.exists { p =>
      if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
}
