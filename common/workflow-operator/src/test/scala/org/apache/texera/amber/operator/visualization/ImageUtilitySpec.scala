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

package org.apache.texera.amber.operator.visualization

import org.scalatest.flatspec.AnyFlatSpec

class ImageUtilitySpec extends AnyFlatSpec {

  "ImageUtility.encodeImageToHTML" should "return a non-empty Python snippet" in {
    assert(ImageUtility.encodeImageToHTML().nonEmpty)
  }

  it should "import base64 for encoding" in {
    assert(ImageUtility.encodeImageToHTML().contains("import base64"))
  }

  it should "base64-encode binary_image_data" in {
    assert(
      ImageUtility.encodeImageToHTML().contains("base64.b64encode(binary_image_data)")
    )
  }

  it should "decode encoded bytes as UTF-8" in {
    assert(ImageUtility.encodeImageToHTML().contains(""".decode("utf-8")"""))
  }

  it should "emit a data-URI img tag via an f-string template" in {
    assert(
      ImageUtility.encodeImageToHTML().contains("data:image;base64,{encoded_image_str}")
    )
  }

  it should "handle binary decode failure with try/except and a render_error fallback" in {
    val snippet = ImageUtility.encodeImageToHTML()
    assert(snippet.contains("except Exception"))
    assert(snippet.contains("Binary input is not valid"))
  }

  it should "assign the img tag to html for downstream yield sites" in {
    assert(ImageUtility.encodeImageToHTML().contains("html = f"))
  }

  it should "be deterministic across repeated calls" in {
    val first = ImageUtility.encodeImageToHTML()
    val second = ImageUtility.encodeImageToHTML()
    assert(first == second)
  }
}
