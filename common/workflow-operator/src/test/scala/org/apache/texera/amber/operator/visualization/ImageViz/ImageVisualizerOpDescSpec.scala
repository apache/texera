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

package org.apache.texera.amber.operator.visualization.ImageViz

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class ImageVisualizerOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {
  var opDesc: ImageVisualizerOpDesc = _
  before {
    opDesc = new ImageVisualizerOpDesc()
  }

  it should "throw AssertionError (not NPE) when binaryContent is left at its default" in {
    // `binaryContent` now defaults to "" instead of null, so an unconfigured
    // operator reaches the nonEmpty assert and surfaces a domain error with
    // the standard "cannot be empty" message rather than a
    // NullPointerException from dereferencing null.
    val ex = intercept[AssertionError] {
      opDesc.createBinaryData()
    }
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
  }

  it should "reject missing binaryContent with a controlled error via generatePythonCode" in {
    // The assert inside createBinaryData is also reachable through the
    // public codegen entry point; the same message must surface there.
    val ex = intercept[AssertionError] {
      opDesc.generatePythonCode()
    }
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
  }

  "ImageVisualizerOpDesc.operatorInfo" should "advertise the user-friendly name and Media group" in {
    val info = opDesc.operatorInfo
    info.userFriendlyName shouldBe "Image Visualizer"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_MEDIA_GROUP
    info.operatorDescription should include("image")
  }

  it should "expose exactly one output port wired through forVisualization" in {
    opDesc.operatorInfo.outputPorts should have length 1
  }

  "ImageVisualizerOpDesc.getOutputSchemas" should "return a single-port schema with an html-content STRING column" in {
    opDesc.binaryContent = "image_bytes"
    val schemas = opDesc.getOutputSchemas(Map.empty)
    schemas should have size 1
    val (portId, schema) = schemas.head
    portId shouldBe opDesc.operatorInfo.outputPorts.head.id
    schema.getAttributes should have length 1
    schema.getAttributes.head.getName shouldBe "html-content"
    schema.getAttributes.head.getType shouldBe AttributeType.STRING
  }

  it should "throw AssertionError with a 'cannot be empty' message when binaryContent is an empty string" in {
    // An explicitly assigned empty string (same as the new default) reaches
    // the nonEmpty assert, whose message must end with the standard
    // "cannot be empty" phrase.
    opDesc.binaryContent = ""
    val ex = intercept[AssertionError](opDesc.createBinaryData())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
  }

  it should "render the configured binary content column in createBinaryData" in {
    opDesc.binaryContent = "image_bytes"
    val plain = opDesc.createBinaryData().plain
    plain should include("image_bytes")
    plain should include("binary_image_data")
  }

  "ImageVisualizerOpDesc.generatePythonCode" should "render a UDFOperatorV2 source with a runtime column-decode site" in {
    // EncodableString fields are NOT emitted as literal strings — the pyb
    // macro wraps them in `self.decode_python_template.decode("<base64>")`
    // calls so the column name resolves at runtime. Verify the structure
    // (operator class, body helper, decode site) instead of a literal name.
    opDesc.binaryContent = "image_bytes"
    val code = opDesc.generatePythonCode()
    code should include("class ProcessTupleOperator(UDFOperatorV2)")
    code should include("encode_image_to_html")
    code should include("decode_python_template")
  }

  "ImageVisualizerOpDesc.generateStandaloneCode" should "avoid literal HTML tags that the UI code viewer can render away" in {
    opDesc.binaryContent = "image_bytes"
    val code = opDesc.generateStandaloneCode()

    code should include("LT = chr(60)")
    code should include("GT = chr(62)")
    code should include("encoded_image_str")
    code should not include "<img"
    code should not include "<h1"
    code should not include "<p"
    code should not include "<div"
  }

  // The executor hands b64encode whatever the column holds, and b64encode refuses
  // a str, so a column of text ends in the reason page. The exported script reads
  // the same column out of JSONL, where a BINARY column arrives as the base64 text
  // of its bytes, so text is the image there and only there.
  private val inPort = new ImageVisualizerOpDesc().operatorInfo.inputPorts.head.id

  private def standaloneFor(columnType: AttributeType): String = {
    opDesc.binaryContent = "image_bytes"
    opDesc.generateStandaloneCode(Map(inPort -> Schema().add("image_bytes", columnType)))
  }

  it should "read the column's text as the image only where it was declared binary" in {
    standaloneFor(AttributeType.BINARY) should include("if isinstance(binary_image_data, str):")
  }

  it should "let a text column fail in b64encode, the way the executor does" in {
    val code = standaloneFor(AttributeType.STRING)
    code should include("if False:")
    code should not include "isinstance(binary_image_data, str)"
    // The call the executor makes on the same value, left to refuse it here too.
    code should include("base64.b64encode(binary_image_data)")
  }

  // Both paths run the same two lines on a string, so the answer is one both can
  // be held to: b64encode raises, and the reason page is what the viewer sees.
  it should "answer a string column with the reason page in both paths" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))

    val body = standaloneFor(AttributeType.STRING).linesIterator
      .takeWhile(!_.startsWith("all_images_html"))
      .mkString("\n")
    val driver =
      s"""$body
         |print(encode_image_to_html("not an image"))
         |""".stripMargin

    val script = Files.createTempFile("image-visualizer-string-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      out should include("Image is not available.")
      out should include("Reason: Binary input is not valid")
      out should not include "img src"
    }
    // The executor's own reason page, which the run showed for the same value.
    opDesc.generatePythonCode() should include("Image is not available.")
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
}
