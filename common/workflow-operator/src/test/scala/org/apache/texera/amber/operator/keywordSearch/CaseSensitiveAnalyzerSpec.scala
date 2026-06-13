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

  private def tokensOf(fieldName: String, input: String): List[String] = {
    val analyzer = new CaseSensitiveAnalyzer
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
      analyzer.close()
    }
    out.toList
  }

  // ---------------------------------------------------------------------------
  // Case preservation — the whole point of CaseSensitiveAnalyzer is to
  // SKIP the lowercasing pipeline used by StandardAnalyzer.
  // ---------------------------------------------------------------------------

  "CaseSensitiveAnalyzer" should "preserve case in every emitted token" in {
    assert(tokensOf("body", "Hello World") == List("Hello", "World"))
  }

  it should "preserve mixed-case tokens (e.g. CamelCase identifiers)" in {
    assert(tokensOf("body", "FooBar BazQux") == List("FooBar", "BazQux"))
  }

  it should "preserve all-uppercase tokens" in {
    assert(tokensOf("body", "URL HTTP HTML") == List("URL", "HTTP", "HTML"))
  }

  it should "preserve all-lowercase tokens (no upcasing either)" in {
    assert(tokensOf("body", "alpha beta gamma") == List("alpha", "beta", "gamma"))
  }

  // ---------------------------------------------------------------------------
  // Whitespace tokenization — the underlying tokenizer is
  // WhitespaceTokenizer; pin its splitting behavior.
  // ---------------------------------------------------------------------------

  "CaseSensitiveAnalyzer (whitespace tokenizer)" should
    "split on a single space" in {
    assert(tokensOf("body", "a b c") == List("a", "b", "c"))
  }

  it should "split on tabs and newlines" in {
    assert(tokensOf("body", "a\tb\nc") == List("a", "b", "c"))
  }

  it should "collapse runs of whitespace (no empty tokens emitted)" in {
    assert(tokensOf("body", "a   b\n\nc") == List("a", "b", "c"))
  }

  // ---------------------------------------------------------------------------
  // Punctuation — WhitespaceTokenizer keeps punctuation attached to
  // adjacent characters (it only splits on whitespace).
  // ---------------------------------------------------------------------------

  it should
    "leave punctuation attached to tokens (WhitespaceTokenizer only splits on whitespace)" in {
    // `"abc,def"` has no whitespace inside, so it stays one token.
    assert(tokensOf("body", "abc,def") == List("abc,def"))
    // Sentence-final punctuation also stays attached.
    assert(tokensOf("body", "Hello, world!") == List("Hello,", "world!"))
  }

  // ---------------------------------------------------------------------------
  // Edge cases
  // ---------------------------------------------------------------------------

  "CaseSensitiveAnalyzer (empty input)" should "produce no tokens" in {
    assert(tokensOf("body", "") == Nil)
  }

  it should "produce no tokens for pure-whitespace input" in {
    assert(tokensOf("body", "   \t\n  ") == Nil)
  }

  // ---------------------------------------------------------------------------
  // StopFilter — empty stop-word set; nothing should be filtered out.
  // ---------------------------------------------------------------------------

  "CaseSensitiveAnalyzer (StopFilter with CharArraySet.EMPTY_SET)" should
    "NOT remove common English stop words (the / and / of / a)" in {
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

  "CaseSensitiveAnalyzer" should
    "produce the same tokens for the same input across different field names" in {
    val a = tokensOf("title", "Hello World")
    val b = tokensOf("body", "Hello World")
    assert(a == b, "field name must not change tokenization")
  }

  it should
    "return independent TokenStreams for successive tokenStream calls on the SAME analyzer instance" in {
    // Reuse one analyzer across two tokenStream calls — consuming the
    // first stream must not affect the second. The helper would create
    // a fresh analyzer per call, masking the intra-analyzer reuse
    // behavior; do the lifecycle manually here.
    val analyzer = new CaseSensitiveAnalyzer
    try {
      def collect(input: String): List[String] = {
        val stream = analyzer.tokenStream("body", new java.io.StringReader(input))
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
      val first = collect("alpha Beta GAMMA")
      val second = collect("alpha Beta GAMMA")
      assert(first == List("alpha", "Beta", "GAMMA"))
      assert(second == first, "second tokenStream call must not be affected by the first")
      // Different input on the SAME analyzer also produces correct tokens.
      val third = collect("foo bar")
      assert(third == List("foo", "bar"))
    } finally {
      analyzer.close()
    }
  }
}
