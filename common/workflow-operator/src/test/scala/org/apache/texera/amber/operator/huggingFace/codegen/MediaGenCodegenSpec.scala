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

package org.apache.texera.amber.operator.huggingFace.codegen

import org.apache.texera.amber.operator.huggingFace.codegen.HuggingFaceCodegenFixtures._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MediaGenCodegenSpec extends AnyFlatSpec with Matchers {

  private val ctx =
    textContext.copy(task = "text-to-image", modelId = "stabilityai/stable-diffusion-2")

  "MediaGenCodegen.task" should "be the canonical text-to-image string" in {
    MediaGenCodegen.task shouldBe "text-to-image"
  }

  "MediaGenCodegen.tasks" should "cover exactly text-to-image and text-to-video" in {
    MediaGenCodegen.tasks shouldBe Set("text-to-image", "text-to-video")
  }

  "MediaGenCodegen.payloadPython" should "emit the static inputs payload shape" in {
    val out = MediaGenCodegen.payloadPython(ctx)
    out should include("""payload = {"inputs": prompt_value}""")
  }

  "MediaGenCodegen.parsePython" should "branch on both media tasks" in {
    val out = MediaGenCodegen.parsePython(ctx)
    out should include("""if task == "text-to-image":""")
    out should include("""elif task == "text-to-video":""")
  }

  it should "route URL responses through the _url_to_data_url helper" in {
    val out = MediaGenCodegen.parsePython(ctx)
    out should include("self._url_to_data_url(")
  }

  it should "convert OpenAI b64_json payloads into a data URL" in {
    val out = MediaGenCodegen.parsePython(ctx)
    out should include("b64_json")
    out should include("data:image/png;base64,")
  }

  it should "fall back to json.dumps(body) when no known shape matches" in {
    val out = MediaGenCodegen.parsePython(ctx)
    out should include("json.dumps(body)")
  }

  "MediaGenCodegen snippets" should "never inline raw CodegenContext string values" in {
    assertNoRawContextValues(MediaGenCodegen)
  }

  it should "produce identical output regardless of the CodegenContext contents" in {
    assertStaticSnippets(MediaGenCodegen, "text-to-image")
  }
}
