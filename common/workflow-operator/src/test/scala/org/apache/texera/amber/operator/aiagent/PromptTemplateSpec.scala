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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.scalatest.flatspec.AnyFlatSpec

class PromptTemplateSpec extends AnyFlatSpec {
  private val schema: Schema = Schema()
    .add(new Attribute("question", AttributeType.STRING))
    .add(new Attribute("count", AttributeType.INTEGER))
    .add(new Attribute("note", AttributeType.STRING))

  private val tuple: Tuple = Tuple
    .builder(schema)
    .add(schema.getAttribute("question"), "How are rows processed?")
    .add(schema.getAttribute("count"), Integer.valueOf(3))
    .add(schema.getAttribute("note"), null)
    .build()

  "PromptTemplate.render" should "replace placeholders with tuple field values" in {
    val rendered = PromptTemplate.render("Q: {{question}} Count: {{count}}", tuple)

    assert(rendered == "Q: How are rows processed? Count: 3")
  }

  it should "support repeated placeholders and whitespace inside delimiters" in {
    val rendered = PromptTemplate.render("{{ question }} -> {{question}}", tuple)

    assert(rendered == "How are rows processed? -> How are rows processed?")
  }

  it should "render null fields as empty strings" in {
    val rendered = PromptTemplate.render("Note={{note}}.", tuple)

    assert(rendered == "Note=.")
  }

  it should "leave templates without placeholders unchanged" in {
    val rendered = PromptTemplate.render("Summarize this row.", tuple)

    assert(rendered == "Summarize this row.")
  }

  it should "return an empty prompt for a null template" in {
    assert(PromptTemplate.render(null, tuple) == "")
  }

  it should "fail when a placeholder references a missing column" in {
    val error = intercept[IllegalArgumentException] {
      PromptTemplate.render("{{missing}}", tuple)
    }

    assert(error.getMessage.contains("missing column: missing"))
  }
}
