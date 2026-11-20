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

package org.apache.texera.amber.operator.keywordSearch

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.scalatest.flatspec.AnyFlatSpec

import java.io.StringReader
import scala.collection.mutable.ArrayBuffer

class CaseSensitiveAnalyzerSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Helper — drive an Analyzer over a single input and collect the emitted
  // token strings.
  //
  // The Lucene TokenStream lifecycle is: reset → while incrementToken → end →
  // close. Skipping reset() raises IllegalStateException on some Lucene
  // versions; we follow the canonical contract to keep the spec robust.
  // ---------------------------------------------------------------------------

  private def tokensOf(
      analyzer: CaseSensitiveAnalyzer,
      fieldName: String,
      input: String
  ): List[String] = {
    val stream = analyzer.tokenStream(fieldName, new StringReader(input))
    val termAttr = stream.addAttribute(classOf[CharTermAttribute])
    val out = ArrayBuffer.empty[String]
    try {
      stream.reset()
      while (stream.incrementToken()) {
        out.append(termAttr.toString)
      }
      stream.end()
    } finally {
      stream.close()
    }
    out.toList
  }

  private def tokensOf(fieldName: String, input: String): List[String] = {
    val analyzer = new CaseSensitiveAnalyzer
    try {
      tokensOf(analyzer, fieldName, input)
    } finally {
      analyzer.close()
    }
  }

  // ---------------------------------------------------------------------------
  // Tokenization test cases — preserves case (skips StandardAnalyzer lowercasing),
  // splits on UAX#29 word boundaries and punctuation, collapses whitespace,
  // and handles empty / whitespace edge cases.
  // ---------------------------------------------------------------------------

  behavior of "CaseSensitiveAnalyzer"

  private val tokenizationCases = Seq(
    // Case preservation
    ("preserve case in every emitted token", "Hello World", List("Hello", "World")),
    (
      "preserve mixed-case tokens (e.g. CamelCase identifiers)",
      "FooBar BazQux",
      List("FooBar", "BazQux")
    ),
    ("preserve all-uppercase tokens", "URL HTTP HTML", List("URL", "HTTP", "HTML")),
    (
      "preserve all-lowercase tokens (no upcasing either)",
      "alpha beta gamma",
      List("alpha", "beta", "gamma")
    ),
    // Word-boundary tokenization & whitespace collapsing (Unicode UAX#29)
    ("split on a single space", "a b c", List("a", "b", "c")),
    ("split on tabs and newlines", "a\tb\nc", List("a", "b", "c")),
    ("collapse runs of whitespace (no empty tokens emitted)", "a   b\n\nc", List("a", "b", "c")),
    // Punctuation word boundaries (punctuation is stripped, not kept attached)
    ("split on comma word boundary and strip punctuation", "abc,def", List("abc", "def")),
    ("split on sentence punctuation and strip it", "Hello, world!", List("Hello", "world")),
    // Edge cases
    ("produce no tokens on empty input", "", Nil),
    ("produce no tokens for pure-whitespace input", "   \t\n  ", Nil)
  )

  for ((description, input, expected) <- tokenizationCases) {
    it should description in {
      assert(tokensOf("body", input) == expected)
    }
  }

  // ---------------------------------------------------------------------------
  // StopFilter — empty stop-word set; nothing should be filtered out.
  // ---------------------------------------------------------------------------

  it should "NOT remove common English stop words (the / and / of / a)" in {
    // StandardAnalyzer's default stop set would strip "the", "and",
    // "of", "a"; this analyzer is built with `CharArraySet.EMPTY_SET`
    // so every token survives. Pin that explicitly.
    val out = tokensOf("body", "the quick and a brown fox jumps of off")
    assert(
      out == List("the", "quick", "and", "a", "brown", "fox", "jumps", "of", "off")
    )
  }

  // ---------------------------------------------------------------------------
  // Field-name independence — tokenStream uses the same pipeline
  // regardless of field name, and each call gets its own TokenStream.
  // ---------------------------------------------------------------------------

  it should "produce the same tokens for the same input across different field names" in {
    val a = tokensOf("title", "Hello World")
    val b = tokensOf("body", "Hello World")
    assert(a == b, "field name must not change tokenization")
  }

  it should "return independent TokenStreams for successive tokenStream calls on the SAME analyzer instance" in {
    // Reuse one analyzer across two tokenStream calls — consuming the
    // first stream must not affect the second.
    val analyzer = new CaseSensitiveAnalyzer
    try {
      val first = tokensOf(analyzer, "body", "alpha Beta GAMMA")
      val second = tokensOf(analyzer, "body", "alpha Beta GAMMA")
      assert(first == List("alpha", "Beta", "GAMMA"))
      assert(second == first, "second tokenStream call must not be affected by the first")
      // Different input on the SAME analyzer also produces correct tokens.
      val third = tokensOf(analyzer, "body", "foo bar")
      assert(third == List("foo", "bar"))
    } finally {
      analyzer.close()
    }
  }
}
