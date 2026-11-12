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

package org.apache.texera.amber.operator.udf.java

import org.apache.texera.amber.core.executor.{ExecFactory, JavaRuntimeCompilation, OpExecWithCode}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.udf.{UiUDFParameter, UiUdfParameterContract}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.Instant
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._

class JavaUdfUiParameterInjectorSpec extends AnyFlatSpec with Matchers {
  private def parameter(name: String, kind: AttributeType, raw: String): UiUDFParameter = {
    val p = new UiUDFParameter
    p.attribute = new Attribute(name, kind)
    p.value = raw
    p
  }
  private def source(field: String): String =
    s"""import org.apache.texera.amber.operator.udf.java.UiParameter;
       |public class JavaUDFOpExec { $field }
       |""".stripMargin
  private def evaluate(code: String, parameters: List[UiUDFParameter]): Any = {
    val cls =
      JavaRuntimeCompilation.compileCode(JavaUdfUiParameterInjector.inject(code, parameters))
    cls.getField("value").get(cls.getDeclaredConstructor().newInstance())
  }

  private val fixture =
    objectMapper.readTree(getClass.getResourceAsStream("/udf/ui-parameter-contract.json"))
  private val examples = fixture.path("values").elements().asScala.toList
  private lazy val validValues = {
    val valid = examples.filterNot(_.path("error").asBoolean()).zipWithIndex
    val fields = valid
      .map {
        case (example, index) =>
          val kind = UiUdfParameterContract.typesByToken(example.path("type").asText())
          s"""@UiParameter("${example
            .path("id")
            .asText()}") public ${kind.getFieldClass.getName} value_$index;"""
      }
      .mkString("\n")
    val parameters = valid.map {
      case (example, _) =>
        parameter(
          example.path("id").asText(),
          UiUdfParameterContract.typesByToken(example.path("type").asText()),
          example.path("raw").asText()
        )
    }
    val cls = JavaRuntimeCompilation.compileCode(
      JavaUdfUiParameterInjector.inject(source(fields), parameters)
    )
    val instance = cls.getDeclaredConstructor().newInstance()
    valid.map {
      case (example, index) =>
        example.path("id").asText() -> cls.getField(s"value_$index").get(instance)
    }.toMap
  }
  for (example <- examples) {
    "Java UI parameter values" should s"honor ${example.path("id").asText()} through javac" in {
      val kind = UiUdfParameterContract.typesByToken(example.path("type").asText())
      val code = source(s"""@UiParameter("x") public ${kind.getFieldClass.getName} value;""")
      val parameters = List(parameter("x", kind, example.path("raw").asText()))
      if (example.path("error").asBoolean()) {
        intercept[RuntimeException](evaluate(code, parameters)).getMessage should include(
          "UiParameter"
        )
      } else {
        val actual = validValues(example.path("id").asText())
        kind match {
          case AttributeType.TIMESTAMP =>
            actual.asInstanceOf[Timestamp].toInstant shouldBe Instant.parse(
              example.path("expected").asText()
            )
          case AttributeType.BOOLEAN => actual shouldBe example.path("expected").asBoolean()
          case AttributeType.DOUBLE =>
            val expected = example.path("expected").asText()
            val number = actual.asInstanceOf[Double]
            if (expected == "nan") number.isNaN shouldBe true
            else if (expected == "inf") number shouldBe Double.PositiveInfinity
            else if (expected == "-inf") number shouldBe Double.NegativeInfinity
            else number shouldBe expected.toDouble
          case _ => actual.toString shouldBe example.path("expected").asText()
        }
      }
    }
  }

  "Java parameter injection" should "leave legacy code unchanged" in {
    JavaUdfUiParameterInjector.inject("return t;", Nil) shouldBe "return t;"
  }

  it should "accept bare templates but refuse saved parameters when javac is unavailable" in {
    val log = Files.createTempFile("java-ui-jre-", ".log")
    val arguments = Files.createTempFile("java-ui-jre-", ".args")
    Files.writeString(
      arguments,
      s"""--limit-modules
         |java.se,jdk.unsupported
         |-cp
         |"${System.getProperty("java.class.path").replace('\\', '/')}"
         |org.apache.texera.amber.operator.udf.java.JavaUiParameterJreProbe
         |""".stripMargin
    )
    val process = new ProcessBuilder(
      Paths.get(System.getProperty("java.home"), "bin", "java").toString,
      s"@$arguments"
    ).redirectErrorStream(true).redirectOutput(log.toFile).start()
    try {
      process.waitFor(30, TimeUnit.SECONDS) shouldBe true
      withClue(Files.readString(log)) { process.exitValue() shouldBe 0 }
    } finally {
      process.destroyForcibly()
      Files.deleteIfExists(log)
      Files.deleteIfExists(arguments)
    }
  }

  it should "keep user text inert and preserve exact signed long limits" in {
    val text = "\"; throw new RuntimeException(); //\n\\u000a\u0000\ud83d\ude80"
    evaluate(
      source("@UiParameter(\"x\") public String value;"),
      List(parameter("x", AttributeType.STRING, text))
    ) shouldBe text
    evaluate(
      source("@UiParameter(\"x\") public long value;"),
      List(parameter("x", AttributeType.LONG, Long.MinValue.toString))
    ) shouldBe Long.MinValue
  }

  for (
    field <- Seq("static int value;", "int value = 1;", "int value, another;", "byte[] value;")
  ) {
    it should s"reject unsupported annotated fields: $field" in {
      intercept[RuntimeException] {
        JavaUdfUiParameterInjector.inject(
          source(s"""@UiParameter("x") $field"""),
          List(parameter("x", AttributeType.INTEGER, "1"))
        )
      }.getMessage should include("UiParameter")
    }
  }

  it should "reject missing, stale, mismatched and duplicate metadata" in {
    val code = source("@UiParameter(\"x\") int value;")
    for (
      parameters <- Seq(
        Nil,
        List(parameter("y", AttributeType.INTEGER, "1")),
        List(parameter("x", AttributeType.STRING, "1")),
        List(parameter("x", AttributeType.INTEGER, "1"), parameter("x", AttributeType.INTEGER, "2"))
      )
    ) {
      intercept[RuntimeException](
        JavaUdfUiParameterInjector.inject(code, parameters)
      ).getMessage should include("UiParameter")
    }
    intercept[RuntimeException](
      JavaUdfUiParameterInjector.inject(
        source("@UiParameter(\"x\") int a; @UiParameter(\"x\") int b;"),
        List(parameter("x", AttributeType.INTEGER, "1"))
      )
    ).getMessage should include("more than once")
  }

  it should "initialize each executor before its constructor and process a real tuple" in {
    val code = """import org.apache.texera.amber.operator.udf.java.UiParameter;
      |import org.apache.texera.amber.operator.map.MapOpExec;
      |import org.apache.texera.amber.core.tuple.*;
      |import scala.Function1;
      |import java.io.Serializable;
      |public class JavaUDFOpExec extends MapOpExec {
      |  @UiParameter("count") private final int count;
      |  public JavaUDFOpExec() {
      |    if (count <= 0) throw new IllegalStateException("parameter unavailable in constructor");
      |    setMapFunc((Function1<Tuple, TupleLike> & Serializable) tuple ->
      |      count == 42 ? tuple : Tuple.builder(tuple.getSchema()).addSequentially(new Object[]{count}).build());
      |  }
      |}
      |""".stripMargin.replace("\n", "\r\n")
    val schema = Schema().add(new Attribute("v", AttributeType.INTEGER))
    val input = Tuple.builder(schema).add(schema.getAttribute("v"), Int.box(42)).build()
    val executors = Seq(42, 7).map { value =>
      val d = new JavaUDFOpDesc
      d.code = code
      d.workers = if (value == 42) 1 else 2
      d.uiParameters = List(parameter("count", AttributeType.INTEGER, value.toString))
      val OpExecWithCode(compiled, language) = d
        .getPhysicalOp(WorkflowIdentity(1), ExecutionIdentity(1))
        .executableOpExecInitInfo: @unchecked
      language shouldBe "java"
      ExecFactory.newExecFromJavaCode(compiled)
    }
    for ((executor, expected) <- executors.zip(Seq(42, 7))) {
      executor.open()
      executor
        .processTuple(input, 0)
        .next()
        .asInstanceOf[Tuple]
        .getField[Int]("v") shouldBe expected
      executor.close()
    }
  }
}

object JavaUiParameterJreProbe {
  def main(args: Array[String]): Unit = {
    assert(javax.tools.ToolProvider.getSystemJavaCompiler == null)
    val parameter = new UiUDFParameter
    parameter.attribute = new Attribute("count", AttributeType.INTEGER)
    parameter.value = "42"
    val code = "public class JavaUDFOpExec { /* @UiParameter */ }"
    assert(JavaUdfUiParameterInjector.inject(code, Nil) == code)
    for (workers <- Seq(1, 2)) {
      val descriptor = new JavaUDFOpDesc
      descriptor.code = code
      descriptor.workers = workers
      val physical = descriptor.getPhysicalOp(WorkflowIdentity(1), ExecutionIdentity(1))
      assert(physical.opExecInitInfo == OpExecWithCode(code, "java"))
    }
    val missingCompiler = scala.util
      .Try(
        JavaUdfUiParameterInjector.inject(
          "public class JavaUDFOpExec { @UiParameter(\"count\") int count; }",
          List(parameter)
        )
      )
      .failed
      .get
    assert(missingCompiler.getMessage.contains("require a JDK"))
    val invalid = scala.util
      .Try(
        JavaUdfUiParameterInjector.inject("", List(parameter, parameter))
      )
      .failed
      .get
    assert(invalid.getMessage.contains("declared more than once"))
  }
}
