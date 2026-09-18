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

package org.apache.texera.amber.operator.visualization.urlviz

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import javax.validation.constraints.NotNull
import scala.io.Source
import scala.util.Try

class UrlVizOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  // ---------------------------------------------------------------------------
  // operatorInfo
  // ---------------------------------------------------------------------------

  "UrlVizOpDesc.operatorInfo" should
    "advertise the URL Visualizer name and Visualization-Media group" in {
    val info = (new UrlVizOpDesc).operatorInfo
    info.userFriendlyName shouldBe "URL Visualizer"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_MEDIA_GROUP
    info.operatorDescription.toLowerCase should include("url")
  }

  // ---------------------------------------------------------------------------
  // getPhysicalOp — wiring + output schema
  // ---------------------------------------------------------------------------

  "UrlVizOpDesc.getPhysicalOp" should
    "wire the UrlVizOpExec class name into the OpExecInitInfo" in {
    val physical = (new UrlVizOpDesc).getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.visualization.urlviz.UrlVizOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
  }

  it should "produce an output schema with a single `html-content` STRING attribute" in {
    val op = new UrlVizOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    // The propagation function ignores its input schemas — it always
    // emits the fixed `html-content: STRING` schema on the (single)
    // output port.
    val out = physical.propagateSchema.func(Map.empty)
    val outputId = op.operatorInfo.outputPorts.head.id
    out.keySet shouldBe Set(outputId)
    val schema: Schema = out(outputId)
    schema.getAttributes should have size 1
    val attr: Attribute = schema.getAttributes.head
    attr.getName shouldBe "html-content"
    attr.getType shouldBe AttributeType.STRING
  }

  // ---------------------------------------------------------------------------
  // Field annotations
  // ---------------------------------------------------------------------------

  "UrlVizOpDesc#urlContentAttrName" should
    "carry @JsonProperty(required = true)" in {
    val jp = classOf[UrlVizOpDesc]
      .getDeclaredField("urlContentAttrName")
      .getAnnotation(classOf[JsonProperty])
    jp should not be null
    jp.required shouldBe true
  }

  it should "carry @AutofillAttributeName (UI populates the attribute dropdown)" in {
    val ann = classOf[UrlVizOpDesc]
      .getDeclaredField("urlContentAttrName")
      .getAnnotation(classOf[AutofillAttributeName])
    ann should not be null
  }

  it should "carry @NotNull (javax.validation contract)" in {
    val notNull = classOf[UrlVizOpDesc]
      .getDeclaredField("urlContentAttrName")
      .getAnnotation(classOf[NotNull])
    notNull should not be null
  }

  "UrlVizOpDesc (class-level)" should
    "carry @JsonSchemaInject restricting `urlContentAttrName` to STRING attributes" in {
    val ann = classOf[UrlVizOpDesc].getAnnotation(classOf[JsonSchemaInject])
    ann should not be null
    val payload = ann.json
    payload should include("attributeTypeRules")
    payload should include("urlContentAttrName")
    payload should include("string")
  }

  // ---------------------------------------------------------------------------
  // Independent instances
  // ---------------------------------------------------------------------------

  "UrlVizOpDesc" should
    "assign a fresh operatorIdentifier per instance (UUID-based id is not shared)" in {
    val a = new UrlVizOpDesc
    val b = new UrlVizOpDesc
    a.operatorIdentifier should not equal b.operatorIdentifier
  }

  // ---------------------------------------------------------------------------
  // The cell lands where an attribute is expected
  // ---------------------------------------------------------------------------

  // A quote in the cell used to close `src=` and leave the rest of it standing as
  // attributes of the iframe, which is the page speaking for whoever wrote the
  // row. Both paths are held to the same written page for that value.
  private val hostileUrl = """https://example.invalid/a" onload="alert(1)"""

  /** A configured descriptor: `urlContentAttrName` is a val, so Jackson writes it. */
  private def descFor(urlAttr: String): UrlVizOpDesc = {
    val node = objectMapper.createObjectNode()
    node.put("operatorType", "URLVisualizer")
    node.put("urlContentAttrName", urlAttr)
    objectMapper.readValue(node.toString, classOf[UrlVizOpDesc])
  }

  it should "escape a hostile cell rather than letting it add an attribute" in {
    val attr = new Attribute("url", AttributeType.STRING)
    val tuple = Tuple.builder(Schema().add(attr)).add(attr, hostileUrl).build()
    val page = new UrlVizOpExec(objectMapper.writeValueAsString(descFor("url")))
      .processTuple(tuple, port = 0)
      .next()
      .getFields
      .head
      .asInstanceOf[String]

    page should include("""src="https://example.invalid/a&quot; onload=&quot;alert(1)"""")
    page should not include """onload="alert"""
  }

  it should "write that cell the same way in the exported script" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val desc = descFor("url")
    val attr = new Attribute("url", AttributeType.STRING)
    val tuple = Tuple.builder(Schema().add(attr)).add(attr, hostileUrl).build()
    val fromExecutor = new UrlVizOpExec(objectMapper.writeValueAsString(desc))
      .processTuple(tuple, port = 0)
      .next()
      .getFields
      .head
      .asInstanceOf[String]

    val driver =
      s"""import pandas as pd
         |
         |in1df = pd.DataFrame({"url": [${pyStringLiteral(hostileUrl)}]})
         |${desc.generateStandaloneCode()}
         |
         |print(out1df["html-content"].iloc[0])
         |""".stripMargin

    val script = Files.createTempFile("urlviz-hostile-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      out.trim shouldBe fromExecutor
    }
  }

  private def resolvePython(): Option[String] = {
    def runnable(exe: String): Boolean =
      Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start()).toOption
        .exists { p =>
          if (!p.waitFor(5, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
          else p.exitValue() == 0
        }

    (sys.env.get("UDF_PYTHON_PATH").filter(_.nonEmpty).toList ++ List(
      "python3",
      "python",
      "py"
    )).distinct
      .find(runnable)
  }

  private def canImportPandas(python: String): Boolean =
    Try(
      new ProcessBuilder(python, "-c", "import pandas").redirectErrorStream(true).start()
    ).toOption.exists { p =>
      if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
}
