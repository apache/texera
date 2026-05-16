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

package org.apache.texera.amber.operator.aiagent

import org.scalatest.flatspec.AnyFlatSpec

class OpenRouterClientSpec extends AnyFlatSpec {

  "OpenRouterClient.parseChatCompletionContent" should "extract the first message content" in {
    val response =
      """
        |{
        |  "id": "gen-1",
        |  "choices": [
        |    {
        |      "message": {
        |        "role": "assistant",
        |        "content": "The answer is row-wise."
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin

    assert(OpenRouterClient.parseChatCompletionContent(response) == "The answer is row-wise.")
  }

  it should "extract an empty assistant message" in {
    val response =
      """
        |{
        |  "choices": [
        |    {
        |      "message": {
        |        "content": ""
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin

    assert(OpenRouterClient.parseChatCompletionContent(response) == "")
  }

  it should "fail when choices is empty" in {
    val error = intercept[RuntimeException] {
      OpenRouterClient.parseChatCompletionContent("""{"choices": []}""")
    }

    assert(error.getMessage.contains("choices[0].message.content"))
  }

  it should "fail when message content is missing" in {
    val error = intercept[RuntimeException] {
      OpenRouterClient.parseChatCompletionContent("""{"choices": [{"message": {}}]}""")
    }

    assert(error.getMessage.contains("choices[0].message.content"))
  }

  it should "fail when the first choice is null" in {
    val error = intercept[RuntimeException] {
      OpenRouterClient.parseChatCompletionContent("""{"choices": [null]}""")
    }

    assert(error.getMessage.contains("choices[0].message.content"))
  }
}
