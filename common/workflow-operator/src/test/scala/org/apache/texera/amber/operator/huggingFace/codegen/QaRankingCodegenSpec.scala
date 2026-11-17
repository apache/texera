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

class QaRankingCodegenSpec extends AnyFlatSpec with Matchers {

  private val ctx = textContext.copy(
    task = "question-answering",
    modelId = "deepset/roberta-base-squad2",
    contextColumn = "context",
    candidateLabels = "positive,negative",
    sentencesColumn = "sentences"
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
    val out = QaRankingCodegen.payloadPython(ctx)
    out should include("""if task == "question-answering":""")
    out should include("""elif task == "table-question-answering":""")
    out should include("""elif task == "zero-shot-classification":""")
    out should include("""elif task == "sentence-similarity":""")
    out should include("""elif task == "text-ranking":""")
    out should include("else:")
  }

  it should "build the question-answering payload from prompt_value and the context column" in {
    val out = QaRankingCodegen.payloadPython(ctx)
    out should include("self.CONTEXT_COLUMN")
    out should include("""payload = {"inputs": {"question": prompt_value, "context": ctx_val}}""")
  }

  it should "route table-question-answering through query and table_dict" in {
    val out = QaRankingCodegen.payloadPython(ctx)
    out should include("""payload = {"inputs": {"query": prompt_value, "table": table_dict}}""")
  }

  it should "derive zero-shot candidate labels from the candidate-labels attribute" in {
    val out = QaRankingCodegen.payloadPython(ctx)
    out should include("self.CANDIDATE_LABELS")
    out should include("candidate_labels")
  }

  it should "split the sentences column for sentence-similarity and text-ranking" in {
    val out = QaRankingCodegen.payloadPython(ctx)
    out should include("self.SENTENCES_COLUMN")
    out should include("source_sentence")
    out should include(""""query": prompt_value""")
    out should include(""""texts": sentences_list""")
  }

  it should "fall back to shipping the raw prompt as inputs" in {
    val out = QaRankingCodegen.payloadPython(ctx)
    out should include("""payload = {"inputs": prompt_value}""")
  }

  "QaRankingCodegen.parsePython" should "extract the answer, including from chat-provider responses" in {
    val out = QaRankingCodegen.parsePython(ctx)
    out should include("""if task == "question-answering":""")
    out should include("""elif task == "table-question-answering":""")
    out should include("""body.get("answer"""")
    // #7195: chat-completions responses (third-party providers) are read from
    // choices[0].message.content, not the native {"answer": ...} shape.
    out should include("""content = self._chat_message_content(body)""")
  }

  it should "degrade instead of raising when a chat response is malformed (#8486)" in {
    // Indexing straight into choices[0]["message"]["content"] raises on a
    // malformed provider response. _parse_response catches KeyError, IndexError
    // and TypeError, but an AttributeError from indexing a non-dict escapes it
    // and that row's cell becomes "Request failed" instead. All three chat
    // extractions now use a truthiness guard plus .get chaining, matching the
    // native shapes beside them, which already degrade via json.dumps(body).
    val out = QaRankingCodegen.parsePython(ctx)
    out should not include ("""["message"]["content"]""")
    out.split("""content = self\._chat_message_content\(body\)""").length - 1 shouldBe 3
  }

  it should "return the raw JSON body for the ranking-style tasks" in {
    val out = QaRankingCodegen.parsePython(ctx)
    out should include(
      """elif task in ("zero-shot-classification", "sentence-similarity", "text-ranking"):"""
    )
    out should include("return json.dumps(body)")
  }

  "QaRankingCodegen snippets" should "never inline raw CodegenContext string values" in {
    assertNoRawContextValues(QaRankingCodegen)
  }

  it should "produce identical output regardless of the CodegenContext contents" in {
    assertStaticSnippets(QaRankingCodegen, "question-answering")
  }

  it should "read chat content through the shared type-checked helper (#8617 review)" in {
    val out = QaRankingCodegen.parsePython(ctx)
    out should include("self._chat_message_content(body)")
    // No direct index/get chaining survives — the helper owns that logic.
    out should not include ("""body["choices"][0]""")
  }
}
