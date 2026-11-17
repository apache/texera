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

class TextGenCodegenSpec extends AnyFlatSpec with Matchers {

  private val ctx = textContext

  "TextGenCodegen.task" should "be the canonical text-generation string" in {
    TextGenCodegen.task shouldBe "text-generation"
  }

  "TextGenCodegen.payloadPython" should "open with the text-generation branch" in {
    val out = TextGenCodegen.payloadPython(ctx)
    out should include("""if task == "text-generation":""")
  }

  it should "emit the OpenAI chat-completions payload shape (messages, max_tokens, temperature)" in {
    val out = TextGenCodegen.payloadPython(ctx)
    out should include("messages")
    out should include("max_tokens")
    out should include("temperature")
  }

  it should "include the else fallback that ships the raw prompt as inputs" in {
    val out = TextGenCodegen.payloadPython(ctx)
    out should include("""payload = {"inputs": prompt_value}""")
  }

  "TextGenCodegen.parsePython" should "pull the assistant text out of the chat response" in {
    // The chat shape (choices[0].message.content) is decoded by the shared,
    // type-checked _chat_message_content helper in HuggingFaceCodegenBase; this
    // snippet's job is to call it and fall back to the raw body.
    val out = TextGenCodegen.parsePython(ctx)
    out should include("content = self._chat_message_content(body)")
    out should include("if content is not None:")
    out should include("return json.dumps(body)")
  }

  it should "degrade instead of raising when a chat response is malformed (#8486)" in {
    // This extraction was fully unguarded: a non-dict body, an empty "choices"
    // list, or a choice missing "message"/"content" raised. _parse_response
    // catches KeyError, IndexError and TypeError, but an AttributeError from
    // indexing a non-dict escapes it and that row reads "Request failed"
    // instead. It now falls back to the raw JSON body, as the other codegens do.
    val out = TextGenCodegen.parsePython(ctx)
    out should include("""content = self._chat_message_content(body)""")
    out should include("if content is not None:")
    out should include("return json.dumps(body)")
    out should not include ("""["message"]["content"]""")
  }

  "TextGenCodegen snippets" should "never inline raw CodegenContext string values" in {
    assertNoRawContextValues(TextGenCodegen)
  }

  it should "produce identical output regardless of the CodegenContext contents" in {
    assertStaticSnippets(TextGenCodegen, "text-generation")
  }

  it should "read chat content through the shared type-checked helper (#8617 review)" in {
    val out = TextGenCodegen.parsePython(ctx)
    out should include("self._chat_message_content(body)")
    // No direct index/get chaining survives — the helper owns that logic.
    out should not include ("""body["choices"][0]""")
  }
}
