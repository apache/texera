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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class SubstringSearchOpExecSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixture builders
  // ---------------------------------------------------------------------------

  private val attr = new Attribute("body", AttributeType.STRING)
  private val schema: Schema = Schema().add(attr)
  private def tuple(text: String): Tuple =
    Tuple.builder(schema).add(attr, text).build()

  private def descJson(substring: String, isCaseSensitive: Boolean = false): String = {
    val desc = new SubstringSearchOpDesc
    desc.attribute = "body"
    desc.substring = substring
    desc.isCaseSensitive = isCaseSensitive
    objectMapper.writeValueAsString(desc)
  }

  // ---------------------------------------------------------------------------
  // Substring detection — match / no-match
  // ---------------------------------------------------------------------------

  "SubstringSearchOpExec" should "yield the input tuple when the substring is present" in {
    val exec = new SubstringSearchOpExec(descJson(substring = "hello"))
    val t = tuple("hello world")
    assert(exec.processTuple(t, port = 0).toList == List(t))
  }

  it should "yield nothing when the substring is absent" in {
    val exec = new SubstringSearchOpExec(descJson(substring = "missing"))
    assert(exec.processTuple(tuple("hello world"), port = 0).toList.isEmpty)
  }

  it should
    "match when the substring sits anywhere in the value (start / middle / end)" in {
    val exec = new SubstringSearchOpExec(descJson(substring = "abc"))
    assert(exec.processTuple(tuple("abc xx"), port = 0).toList.nonEmpty)
    assert(exec.processTuple(tuple("xx abc xx"), port = 0).toList.nonEmpty)
    assert(exec.processTuple(tuple("xx abc"), port = 0).toList.nonEmpty)
  }

  // ---------------------------------------------------------------------------
  // Case sensitivity
  // ---------------------------------------------------------------------------

  "SubstringSearchOpExec with isCaseSensitive = true" should
    "match case-sensitively (case mismatch is rejected)" in {
    val exec = new SubstringSearchOpExec(descJson(substring = "HELLO", isCaseSensitive = true))
    assert(exec.processTuple(tuple("hello world"), port = 0).toList.isEmpty)
  }

  it should "yield the tuple when the case matches under case-sensitive mode" in {
    val exec = new SubstringSearchOpExec(descJson(substring = "HELLO", isCaseSensitive = true))
    val t = tuple("Say HELLO loudly")
    assert(exec.processTuple(t, port = 0).toList == List(t))
  }

  "SubstringSearchOpExec with isCaseSensitive = false" should
    "match case-insensitively (production lowercases both sides before String.contains)" in {
    val exec = new SubstringSearchOpExec(descJson(substring = "HELLO", isCaseSensitive = false))
    val t = tuple("hello world")
    assert(exec.processTuple(t, port = 0).toList == List(t))
  }

  it should "still match identical-case values under case-insensitive mode" in {
    val exec = new SubstringSearchOpExec(descJson(substring = "world", isCaseSensitive = false))
    val t = tuple("hello world")
    assert(exec.processTuple(t, port = 0).toList == List(t))
  }

  // ---------------------------------------------------------------------------
  // Edge: empty substring
  // ---------------------------------------------------------------------------

  it should "treat the empty substring as matching every value (Java String.contains(\"\") == true)" in {
    val exec = new SubstringSearchOpExec(descJson(substring = ""))
    val t = tuple("any non-empty text")
    assert(exec.processTuple(t, port = 0).toList == List(t))
    // Even an empty value contains the empty substring.
    val empty = tuple("")
    assert(exec.processTuple(empty, port = 0).toList == List(empty))
  }

  // ---------------------------------------------------------------------------
  // Repeated invocations — predicate stays stable
  // ---------------------------------------------------------------------------

  it should "produce stable results across repeated processTuple calls" in {
    val exec = new SubstringSearchOpExec(descJson(substring = "match"))
    val hit = tuple("match here")
    val miss = tuple("no signal")
    assert(exec.processTuple(hit, port = 0).toList == List(hit))
    assert(exec.processTuple(miss, port = 0).toList.isEmpty)
    assert(exec.processTuple(hit, port = 0).toList == List(hit))
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
