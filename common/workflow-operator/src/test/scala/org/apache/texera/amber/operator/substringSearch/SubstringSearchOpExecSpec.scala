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

package org.apache.texera.amber.operator.substringSearch

import org.apache.texera.amber.operator.FilterSearchSpecSupport
import org.apache.texera.amber.util.JSONUtils.objectMapper

class SubstringSearchOpExecSpec extends FilterSearchSpecSupport {

  private def execFor(
      substring: String,
      isCaseSensitive: Boolean = false
  ): SubstringSearchOpExec = {
    val desc = new SubstringSearchOpDesc
    desc.attribute = "body"
    desc.substring = substring
    desc.isCaseSensitive = isCaseSensitive
    new SubstringSearchOpExec(objectMapper.writeValueAsString(desc))
  }

  // ---------------------------------------------------------------------------
  // Substring detection — match / no-match
  // ---------------------------------------------------------------------------

  "SubstringSearchOpExec" should "yield the input tuple when the substring is present" in {
    assertMatches(execFor("hello"), "hello world")
  }

  it should "yield nothing when the substring is absent" in {
    assertMisses(execFor("missing"), "hello world")
  }

  it should
    "match when the substring sits anywhere in the value (start / middle / end)" in {
    val exec = execFor("abc")
    assert(exec.processTuple(tuple("abc xx"), port = 0).toList.nonEmpty)
    assert(exec.processTuple(tuple("xx abc xx"), port = 0).toList.nonEmpty)
    assert(exec.processTuple(tuple("xx abc"), port = 0).toList.nonEmpty)
  }

  // ---------------------------------------------------------------------------
  // Case sensitivity
  // ---------------------------------------------------------------------------

  "SubstringSearchOpExec with isCaseSensitive = true" should
    "match case-sensitively (case mismatch is rejected)" in {
    assertMisses(execFor("HELLO", isCaseSensitive = true), "hello world")
  }

  it should "yield the tuple when the case matches under case-sensitive mode" in {
    assertMatches(execFor("HELLO", isCaseSensitive = true), "Say HELLO loudly")
  }

  "SubstringSearchOpExec with isCaseSensitive = false" should
    "match case-insensitively (production lowercases both sides before String.contains)" in {
    assertMatches(execFor("HELLO", isCaseSensitive = false), "hello world")
  }

  it should "still match identical-case values under case-insensitive mode" in {
    assertMatches(execFor("world", isCaseSensitive = false), "hello world")
  }

  // ---------------------------------------------------------------------------
  // Edge: empty cell
  // ---------------------------------------------------------------------------

  it should "yield nothing when the column is empty" in {
    // A blank CSV cell arrives as null. This used to throw a NullPointerException on
    // the toString instead of answering the filter.
    assertMisses(execFor("hello"), null)
  }

  it should "yield nothing when the column is empty and the substring is empty too" in {
    // The empty substring matches every value, but a row with no value has none to
    // match, so it is filtered out rather than kept.
    assertMisses(execFor(""), null)
  }

  // ---------------------------------------------------------------------------
  // Edge: empty substring
  // ---------------------------------------------------------------------------

  it should "treat the empty substring as matching every value (Java String.contains(\"\") == true)" in {
    val exec = execFor("")
    assertMatches(exec, "any non-empty text")
    // Even an empty value contains the empty substring.
    assertMatches(exec, "")
  }

  // ---------------------------------------------------------------------------
  // Repeated invocations — predicate stays stable
  // ---------------------------------------------------------------------------

  it should "produce stable results across repeated processTuple calls" in {
    val exec = execFor("match")
    assertMatches(exec, "match here")
    assertMisses(exec, "no signal")
    assertMatches(exec, "match here")
  }

  // ---------------------------------------------------------------------------
  // Descriptor parse failure
  // ---------------------------------------------------------------------------

  "SubstringSearchOpExec construction" should
    "throw on malformed descriptor JSON" in {
    intercept[com.fasterxml.jackson.core.JsonProcessingException] {
      new SubstringSearchOpExec("{not valid")
    }
  }
}
