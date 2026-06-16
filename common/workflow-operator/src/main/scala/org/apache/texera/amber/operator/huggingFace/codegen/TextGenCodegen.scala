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

/**
  * Codegen for the `text-generation` Hugging Face pipeline task.
  *
  * The payload is the OpenAI chat-completions shape — `messages` with a
  * system + user pair plus `max_tokens` / `temperature` knobs — which is
  * what the HF router and every OpenAI-compatible third-party provider
  * (Cerebras, Groq, Sambanova, Together, …) accepts.
  *
  * The parse step pulls `body["choices"][0]["message"]["content"]` out of
  * the response.
  */
object TextGenCodegen extends TaskCodegen {

  override val task: String = "text-generation"

  override def payloadPython(ctx: CodegenContext): String =
    """            if task == "text-generation":
      |                payload = {
      |                    "model": self.MODEL_ID,
      |                    "messages": [
      |                        {"role": "system", "content": self.SYSTEM_PROMPT},
      |                        {"role": "user", "content": prompt_value},
      |                    ],
      |                    "max_tokens": self.MAX_NEW_TOKENS,
      |                    "temperature": self.TEMPERATURE,
      |                }
      |            else:
      |                payload = {"inputs": prompt_value}""".stripMargin

  override def parsePython(ctx: CodegenContext): String =
    """            if task == "text-generation":
      |                return body["choices"][0]["message"]["content"]""".stripMargin
}
