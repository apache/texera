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

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
}
