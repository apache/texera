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

import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class QaRankingCodegenSpec extends AnyFlatSpec with Matchers {

  private def makeCtx(
      hfApiToken: EncodableString = "token",
      modelId: EncodableString = "deepset/roberta-base-squad2",
      promptColumn: EncodableString = "prompt",
      resultColumn: EncodableString = "hf_response",
      task: EncodableString = "question-answering",
      systemPrompt: EncodableString = "You are a helpful assistant.",
      safeMaxTokens: Int = 256,
      safeTemp: Double = 0.7,
      contextColumn: EncodableString = "context",
      candidateLabels: EncodableString = "positive,negative",
      sentencesColumn: EncodableString = "sentences"
  ): CodegenContext =
    CodegenContext(
      hfApiToken = hfApiToken,
      modelId = modelId,
      promptColumn = promptColumn,
      resultColumn = resultColumn,
      task = task,
      systemPrompt = systemPrompt,
      safeMaxTokens = safeMaxTokens,
      safeTemp = safeTemp,
      contextColumn = contextColumn,
      candidateLabels = candidateLabels,
      sentencesColumn = sentencesColumn
    )

  "QaRankingCodegen.task" should "be the canonical question-answering string" in {
    QaRankingCodegen.task shouldBe "question-answering"
  }

  "QaRankingCodegen.tasks" should "cover exactly the five QA/ranking task families" in {
    QaRankingCodegen.tasks shouldBe Set(
      "question-answering",
      "table-question-answering",
      "zero-shot-classification",
      "sentence-similarity",
      "text-ranking"
    )
  }

  it should "include its primary task among the handled tasks" in {
    QaRankingCodegen.tasks should contain(QaRankingCodegen.task)
  }

  "QaRankingCodegen.payloadPython" should "branch on each of the five tasks and an else fallback" in {
    val out = QaRankingCodegen.payloadPython(makeCtx())
    out should include("""if task == "question-answering":""")
    out should include("""elif task == "table-question-answering":""")
    out should include("""elif task == "zero-shot-classification":""")
    out should include("""elif task == "sentence-similarity":""")
    out should include("""elif task == "text-ranking":""")
    out should include("else:")
  }

  it should "build the question-answering payload from prompt_value and the context column" in {
    val out = QaRankingCodegen.payloadPython(makeCtx())
    out should include("self.CONTEXT_COLUMN")
    out should include("""payload = {"inputs": {"question": prompt_value, "context": ctx_val}}""")
  }

  it should "route table-question-answering through query and table_dict" in {
    val out = QaRankingCodegen.payloadPython(makeCtx())
    out should include("""payload = {"inputs": {"query": prompt_value, "table": table_dict}}""")
  }

  it should "derive zero-shot candidate labels from the candidate-labels attribute" in {
    val out = QaRankingCodegen.payloadPython(makeCtx())
    out should include("self.CANDIDATE_LABELS")
    out should include("candidate_labels")
  }

  it should "split the sentences column for sentence-similarity and text-ranking" in {
    val out = QaRankingCodegen.payloadPython(makeCtx())
    out should include("self.SENTENCES_COLUMN")
    out should include("source_sentence")
    out should include(""""query": prompt_value""")
    out should include(""""texts": sentences_list""")
  }

  it should "fall back to shipping the raw prompt as inputs" in {
    val out = QaRankingCodegen.payloadPython(makeCtx())
    out should include("""payload = {"inputs": prompt_value}""")
  }

  "QaRankingCodegen.parsePython" should "extract the answer field for both QA variants" in {
    val out = QaRankingCodegen.parsePython(makeCtx())
    out should include("""if task == "question-answering":""")
    out should include("""elif task == "table-question-answering":""")
    out should include("""body.get("answer"""")
  }

  it should "return the raw JSON body for the ranking-style tasks" in {
    val out = QaRankingCodegen.parsePython(makeCtx())
    out should include(
      """elif task in ("zero-shot-classification", "sentence-similarity", "text-ranking"):"""
    )
    out should include("return json.dumps(body)")
  }

  "QaRankingCodegen snippets" should "never inline raw CodegenContext string values" in {
    // The snippets reference only self.* attributes; the base class decodes
    // user-supplied strings safely at runtime. Sentinel values are distinctive
    // and non-overlapping with the static template text.
    val ctx = makeCtx(
      hfApiToken = "MARKER_TOKEN_zXyq42",
      modelId = "MARKER_MODEL_zXyq42",
      promptColumn = "MARKER_PROMPT_zXyq42",
      resultColumn = "MARKER_RESULT_zXyq42",
      task = "MARKER_TASK_zXyq42",
      systemPrompt = "MARKER_SYSTEM_zXyq42",
      contextColumn = "MARKER_CONTEXT_zXyq42",
      candidateLabels = "MARKER_LABELS_zXyq42",
      sentencesColumn = "MARKER_SENTENCES_zXyq42"
    )
    val payload = QaRankingCodegen.payloadPython(ctx)
    val parse = QaRankingCodegen.parsePython(ctx)

    for (
      marker <- Seq(
        "MARKER_TOKEN_zXyq42",
        "MARKER_MODEL_zXyq42",
        "MARKER_PROMPT_zXyq42",
        "MARKER_RESULT_zXyq42",
        "MARKER_TASK_zXyq42",
        "MARKER_SYSTEM_zXyq42",
        "MARKER_CONTEXT_zXyq42",
        "MARKER_LABELS_zXyq42",
        "MARKER_SENTENCES_zXyq42"
      )
    ) {
      payload should not include marker
      parse should not include marker
    }
  }

  it should "produce identical output regardless of the CodegenContext contents" in {
    // The payload/parse snippets are static: they reference only self.*
    // attributes, never ctx fields. Two unrelated contexts must serialise to
    // byte-identical Python. A future refactor that accidentally consumes a
    // ctx field will regress here.
    val ctxA = makeCtx(
      hfApiToken = "token-A",
      modelId = "model-A",
      promptColumn = "col-A",
      resultColumn = "result-A",
      systemPrompt = "system-A",
      contextColumn = "ctx-A",
      candidateLabels = "labels-A",
      sentencesColumn = "sent-A",
      safeMaxTokens = 1,
      safeTemp = 0.0
    )
    val ctxB = makeCtx(
      hfApiToken = "token-B",
      modelId = "model-B",
      promptColumn = "col-B",
      resultColumn = "result-B",
      systemPrompt = "system-B",
      contextColumn = "ctx-B",
      candidateLabels = "labels-B",
      sentencesColumn = "sent-B",
      safeMaxTokens = 4096,
      safeTemp = 2.0
    )

    QaRankingCodegen.payloadPython(ctxA) shouldBe QaRankingCodegen.payloadPython(ctxB)
    QaRankingCodegen.parsePython(ctxA) shouldBe QaRankingCodegen.parsePython(ctxB)
  }
}
