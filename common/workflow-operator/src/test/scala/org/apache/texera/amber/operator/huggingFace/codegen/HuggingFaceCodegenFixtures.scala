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

import org.scalatest.matchers.should.Matchers

private[codegen] object HuggingFaceCodegenFixtures extends Matchers {
  val textContext: CodegenContext = CodegenContext(
    hfApiToken = "token",
    modelId = "Qwen/Qwen2.5-72B-Instruct",
    promptColumn = "prompt",
    resultColumn = "hf_response",
    task = "text-generation",
    systemPrompt = "You are a helpful assistant.",
    safeMaxTokens = 256,
    safeTemp = 0.7
  )

  private def variedContext(suffix: String, maxTokens: Int, temperature: Double): CodegenContext =
    CodegenContext(
      hfApiToken = s"token-$suffix",
      modelId = s"model-$suffix",
      promptColumn = s"col-$suffix",
      resultColumn = s"result-$suffix",
      task = s"task-$suffix",
      systemPrompt = s"system-$suffix",
      safeMaxTokens = maxTokens,
      safeTemp = temperature,
      imageInput = s"image-$suffix",
      inputImageColumn = s"image-col-$suffix",
      audioInput = s"audio-$suffix",
      inputAudioColumn = s"audio-col-$suffix",
      contextColumn = s"ctx-$suffix",
      candidateLabels = s"labels-$suffix",
      sentencesColumn = s"sent-$suffix"
    )

  def assertNoRawContextValues(codegen: TaskCodegen): Unit = {
    val ctx = variedContext("MARKER_zXyq42", 256, 0.7)
    val payload = codegen.payloadPython(ctx)
    val parse = codegen.parsePython(ctx)
    // Probe every string field in the fixture, including fields specific to other task families.
    ctx.productIterator.collect { case marker: String => marker }.foreach { marker =>
      payload should not include marker
      parse should not include marker
    }
  }

  def assertStaticSnippets(
      codegen: TaskCodegen,
      originalTask: String,
      otherTask: Option[String] = None
  ): Unit = {
    val a = variedContext("A", 1, 0.0).copy(task = originalTask)
    val b = variedContext("B", 4096, 2.0).copy(task = otherTask.getOrElse(originalTask))
    codegen.payloadPython(a) shouldBe codegen.payloadPython(b)
    codegen.parsePython(a) shouldBe codegen.parsePython(b)
  }
}
