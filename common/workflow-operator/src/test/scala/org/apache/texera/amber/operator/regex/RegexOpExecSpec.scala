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

package org.apache.texera.amber.operator.regex

import org.apache.texera.amber.operator.FilterSearchSpecSupport
import org.apache.texera.amber.util.JSONUtils.objectMapper

class RegexOpExecSpec extends FilterSearchSpecSupport {

  private def execFor(regex: String, caseInsensitive: Boolean = false): RegexOpExec = {
    val desc = new RegexOpDesc
    desc.attribute = "body"
    desc.regex = regex
    desc.caseInsensitive = caseInsensitive
    new RegexOpExec(objectMapper.writeValueAsString(desc))
  }

  // ---------------------------------------------------------------------------
  // Pattern matching — find-semantics (substring match, not full match)
  // ---------------------------------------------------------------------------

  "RegexOpExec" should "yield the input tuple when the regex matches the attribute" in {
    assertMatches(execFor("hello"), "hello world")
  }

  it should "use find-semantics — a pattern matches if it appears anywhere in the value" in {
    // `Matcher.find` succeeds on a substring; it is not anchored. Pin
    // this so a future refactor that switched to `matches` (full-string)
    // would surface here.
    assertMatches(execFor("abc"), "xx abc xx")
  }

  it should "yield nothing when the regex does not match" in {
    assertMisses(execFor("foo"), "bar baz")
  }

  it should "yield the tuple when the regex character class matches at least one char" in {
    val exec = execFor("\\d+")
    assert(exec.processTuple(tuple("answer is 42 plus"), port = 0).toList.size == 1)
  }

  // ---------------------------------------------------------------------------
  // Case sensitivity
  // ---------------------------------------------------------------------------

  "RegexOpExec with caseInsensitive = true" should "match case-insensitively" in {
    assertMatches(execFor("HELLO", caseInsensitive = true), "hello world")
  }

  "RegexOpExec with caseInsensitive = false" should
    "NOT match when the case differs (default behavior)" in {
    assertMisses(execFor("HELLO", caseInsensitive = false), "hello world")
  }

  it should "still match identical case under case-sensitive mode" in {
    assertMatches(execFor("HELLO", caseInsensitive = false), "Say HELLO!")
  }

  // ---------------------------------------------------------------------------
  // Pattern compilation laziness — `pattern` is a lazy val; pin that
  // construction does not eagerly compile (so a bad regex doesn't blow
  // up at the wrong time).
  // ---------------------------------------------------------------------------

  "RegexOpExec" should
    "tolerate construction with an invalid regex (compilation is lazy on `pattern`)" in {
    // `[` is an invalid character class — but the pattern is lazily
    // compiled inside `matchRegex`. The constructor must succeed; the
    // failure only surfaces on the first processTuple call.
    val exec = execFor("[")
    intercept[java.util.regex.PatternSyntaxException] {
      exec.processTuple(tuple("anything"), port = 0).toList
    }
  }

  // ---------------------------------------------------------------------------
  // Repeated invocations — pattern stays cached (lazy val), behavior stable
  // ---------------------------------------------------------------------------

  it should "produce stable results across repeated processTuple calls (pattern cached)" in {
    val exec = execFor("match")
    assertMatches(exec, "match here")
    assertMisses(exec, "no signal")
    assertMatches(exec, "match here")
  }

  // ---------------------------------------------------------------------------
  // Descriptor parse failure surfaces during construction
  // ---------------------------------------------------------------------------

  it should "yield nothing when the column is empty" in {
    // A blank CSV cell arrives as null. This used to throw a NullPointerException on
    // the toString instead of answering the filter.
    assertMisses(execFor("hello"), null)
  }

  it should "yield nothing when the column is empty and the regex matches anything" in {
    // `.*` finds a match in every value, including the empty string, but a row with
    // no value has none to search, so it is filtered out rather than kept.
    assertMisses(execFor(".*"), null)
  }

  "RegexOpExec construction" should
    "throw on malformed descriptor JSON" in {
    // The constructor calls objectMapper.readValue; mis-formed JSON must
    // propagate as a Jackson parse exception, not silently fall through
    // to a half-constructed executor.
    intercept[com.fasterxml.jackson.core.JsonProcessingException] {
      new RegexOpExec("{not valid json")
    }
  }
}
