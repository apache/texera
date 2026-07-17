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
  * Codegen for question-answering, zero-shot, similarity, and ranking tasks.
  *
  * These tasks are prompt-driven but need extra per-row or per-operator
  * inputs: context text, candidate labels, table contents, or a list of
  * comparison sentences/documents.
  */
object QaRankingCodegen extends TaskCodegen {

  override val task: String = "question-answering"

  override val tasks: Set[String] = Set(
    "question-answering",
    "table-question-answering",
    "zero-shot-classification",
    "sentence-similarity",
    "text-ranking"
  )

  override def payloadPython(ctx: CodegenContext): String =
    """            if task == "question-answering":
      |                ctx_val = row[self.CONTEXT_COLUMN]
      |                ctx_val = "" if pd.isna(ctx_val) else str(ctx_val)
      |                payload = {"inputs": {"question": prompt_value, "context": ctx_val}}
      |            elif task == "table-question-answering":
      |                payload = {"inputs": {"query": prompt_value, "table": table_dict}}
      |            elif task == "zero-shot-classification":
      |                labels = [l.strip() for l in str(self.CANDIDATE_LABELS).split(",") if l.strip()]
      |                payload = {
      |                    "inputs": prompt_value,
      |                    "parameters": {"candidate_labels": labels},
      |                }
      |            elif task == "sentence-similarity":
      |                sent_val = row[self.SENTENCES_COLUMN]
      |                sent_val = "" if pd.isna(sent_val) else str(sent_val)
      |                sentences_list = [s.strip() for s in sent_val.split(",") if s.strip()]
      |                payload = {
      |                    "inputs": {
      |                        "source_sentence": prompt_value,
      |                        "sentences": sentences_list,
      |                    }
      |                }
      |            elif task == "text-ranking":
      |                sent_val = row[self.SENTENCES_COLUMN]
      |                sent_val = "" if pd.isna(sent_val) else str(sent_val)
      |                sentences_list = [s.strip() for s in sent_val.split(",") if s.strip()]
      |                payload = {
      |                    "inputs": {
      |                        "query": prompt_value,
      |                        "texts": sentences_list,
      |                    }
      |                }
      |            else:
      |                payload = {"inputs": prompt_value}""".stripMargin

  override def parsePython(ctx: CodegenContext): String =
    """            if task == "question-answering":
      |                return body.get("answer", json.dumps(body)) if isinstance(body, dict) else json.dumps(body)
      |            elif task == "table-question-answering":
      |                return body.get("answer", json.dumps(body)) if isinstance(body, dict) else json.dumps(body)
      |            elif task in ("zero-shot-classification", "sentence-similarity", "text-ranking"):
      |                return json.dumps(body)""".stripMargin
}
