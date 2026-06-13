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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class RegexOpExecSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixture builders — mirrors the pattern used by SpecializedFilterOpExecSpec
  // ---------------------------------------------------------------------------

  private val attr = new Attribute("body", AttributeType.STRING)
  private val schema: Schema = Schema().add(attr)
  private def tuple(text: String): Tuple =
    Tuple.builder(schema).add(attr, text).build()

  private def descJson(regex: String, caseInsensitive: Boolean = false): String = {
    val desc = new RegexOpDesc
    desc.attribute = "body"
    desc.regex = regex
    desc.caseInsensitive = caseInsensitive
    objectMapper.writeValueAsString(desc)
  }

  // ---------------------------------------------------------------------------
  // Pattern matching — find-semantics (substring match, not full match)
  // ---------------------------------------------------------------------------

  "RegexOpExec" should "yield the input tuple when the regex matches the attribute" in {
    val exec = new RegexOpExec(descJson(regex = "hello"))
    val t = tuple("hello world")
    assert(exec.processTuple(t, port = 0).toList == List(t))
  }

  it should "use find-semantics — a pattern matches if it appears anywhere in the value" in {
    // `Matcher.find` succeeds on a substring; it is not anchored. Pin
    // this so a future refactor that switched to `matches` (full-string)
    // would surface here.
    val exec = new RegexOpExec(descJson(regex = "abc"))
    val t = tuple("xx abc xx")
    assert(exec.processTuple(t, port = 0).toList == List(t))
  }

  it should "yield nothing when the regex does not match" in {
    val exec = new RegexOpExec(descJson(regex = "foo"))
    assert(exec.processTuple(tuple("bar baz"), port = 0).toList.isEmpty)
  }

  it should "yield the tuple when the regex character class matches at least one char" in {
    val exec = new RegexOpExec(descJson(regex = "\\d+"))
    assert(exec.processTuple(tuple("answer is 42 plus"), port = 0).toList.size == 1)
  }

  // ---------------------------------------------------------------------------
  // Case sensitivity
  // ---------------------------------------------------------------------------

  "RegexOpExec with caseInsensitive = true" should "match case-insensitively" in {
    val exec = new RegexOpExec(descJson(regex = "HELLO", caseInsensitive = true))
    val t = tuple("hello world")
    assert(exec.processTuple(t, port = 0).toList == List(t))
  }

  "RegexOpExec with caseInsensitive = false" should
    "NOT match when the case differs (default behavior)" in {
    val exec = new RegexOpExec(descJson(regex = "HELLO", caseInsensitive = false))
    assert(exec.processTuple(tuple("hello world"), port = 0).toList.isEmpty)
  }

  it should "still match identical case under case-sensitive mode" in {
    val exec = new RegexOpExec(descJson(regex = "HELLO", caseInsensitive = false))
    val t = tuple("Say HELLO!")
    assert(exec.processTuple(t, port = 0).toList == List(t))
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
    val exec = new RegexOpExec(descJson(regex = "["))
    intercept[java.util.regex.PatternSyntaxException] {
      exec.processTuple(tuple("anything"), port = 0).toList
    }
  }

  // ---------------------------------------------------------------------------
  // Repeated invocations — pattern stays cached (lazy val), behavior stable
  // ---------------------------------------------------------------------------

  it should "produce stable results across repeated processTuple calls (pattern cached)" in {
    val exec = new RegexOpExec(descJson(regex = "match"))
    val hit = tuple("match here")
    val miss = tuple("no signal")
    assert(exec.processTuple(hit, port = 0).toList == List(hit))
    assert(exec.processTuple(miss, port = 0).toList.isEmpty)
    assert(exec.processTuple(hit, port = 0).toList == List(hit))
  }

  // ---------------------------------------------------------------------------
  // Descriptor parse failure surfaces during construction
  // ---------------------------------------------------------------------------

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
