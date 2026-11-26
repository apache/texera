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

package org.apache.texera.web.resource.dashboard.hub

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class HubEntityModelSpec extends AnyFlatSpec {

  private val actions: Seq[(ActionType, String, String)] = Seq(
    (ActionType.View, "view", "VIEW"),
    (ActionType.Like, "like", "Like"),
    (ActionType.Clone, "clone", "CLONE"),
    (ActionType.Unlike, "unlike", "uNlIkE")
  )

  // ---------------------------------------------------------------------------
  // ActionType
  // ---------------------------------------------------------------------------

  "ActionType subtypes" should "expose their lowercase string value" in {
    actions.foreach { case (kind, wire, _) => assert(kind.value == wire) }
  }

  it should "have toString equal to value (override pin)" in {
    // The trait override `toString = value` is what reaches log lines and
    // tracking pipelines. Pin so a regression that returns the case-object
    // name (e.g. "View" instead of "view") breaks here. Use the class
    // simpleName in the failure message — not toString, which is what
    // we are pinning — so a regression produces a readable diagnostic.
    actions.foreach {
      case (a, _, _) =>
        val name = a.getClass.getSimpleName
        assert(a.toString == a.value, s"$name.toString = '${a.toString}' but value = '${a.value}'")
    }
  }

  "ActionType.fromString" should "match each subtype exactly" in {
    actions.foreach { case (kind, wire, _) => assert(ActionType.fromString(wire) == kind) }
  }

  it should "match case-insensitively" in {
    actions.foreach { case (kind, _, mixed) => assert(ActionType.fromString(mixed) == kind) }
  }

  it should "throw IllegalArgumentException for an unknown action, naming the input" in {
    val ex = intercept[IllegalArgumentException] {
      ActionType.fromString("delete")
    }
    assert(ex.getMessage.contains("delete"), s"unexpected message: ${ex.getMessage}")
  }

  it should "throw IllegalArgumentException for an empty string, naming the empty input" in {
    // Pin the concrete `''` representation in the message, not just that
    // an exception is thrown (`"".contains("")` is trivially true).
    val ex = intercept[IllegalArgumentException] {
      ActionType.fromString("")
    }
    assert(ex.getMessage.contains("''"), s"unexpected message: ${ex.getMessage}")
  }

  "ActionType Jackson round-trip" should "serialize each subtype as its lowercase string value" in {
    // `@JsonValue` on `value` instructs Jackson to emit the field's value
    // string instead of a wrapping object form.
    actions.foreach {
      case (action, wire, _) =>
        assert(objectMapper.writeValueAsString(action) == s""""$wire"""")
    }
  }

  it should "deserialize from the lowercase string back to the canonical subtype" in {
    // `@JsonCreator` on `fromString` lets Jackson reconstruct the subtype
    // from a raw string field.
    actions.foreach {
      case (action, wire, _) =>
        assert(objectMapper.readValue(s""""$wire"""", classOf[ActionType]) == action)
    }
  }

  it should "honor the case-insensitive deserialization via @JsonCreator" in {
    assert(objectMapper.readValue("\"VIEW\"", classOf[ActionType]) == ActionType.View)
  }

}
