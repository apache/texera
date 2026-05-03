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

package org.apache.texera.amber.util

import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JSONUtilsSpec extends AnyFlatSpec with Matchers {

  private def parse(json: String): JsonNode = JSONUtils.objectMapper.readTree(json)

  // ----- non-flatten mode (default) -----

  "JSONToMap" should "return an empty map for an empty object" in {
    JSONUtils.JSONToMap(parse("{}")) shouldBe Map.empty[String, String]
  }

  it should "return only first-level primitives by default" in {
    val node = parse("""{"a":"x","b":1,"c":2.5,"d":true}""")
    JSONUtils.JSONToMap(node) shouldBe Map(
      "a" -> "x",
      "b" -> "1",
      "c" -> "2.5",
      "d" -> "true"
    )
  }

  it should "render JSON null as the literal string \"null\" for top-level fields" in {
    val node = parse("""{"a":null}""")
    JSONUtils.JSONToMap(node) shouldBe Map("a" -> "null")
  }

  it should "skip nested objects when flatten=false" in {
    val node = parse("""{"a":"x","nested":{"k":"v"}}""")
    JSONUtils.JSONToMap(node, flatten = false) shouldBe Map("a" -> "x")
  }

  it should "skip nested arrays when flatten=false" in {
    val node = parse("""{"a":"x","arr":[1,2,3]}""")
    JSONUtils.JSONToMap(node, flatten = false) shouldBe Map("a" -> "x")
  }

  // ----- flatten mode -----

  it should "flatten a nested object with parent.child keys when flatten=true" in {
    val node = parse("""{"a":"x","nested":{"k":"v","deep":{"z":"y"}}}""")
    JSONUtils.JSONToMap(node, flatten = true) shouldBe Map(
      "a" -> "x",
      "nested.k" -> "v",
      "nested.deep.z" -> "y"
    )
  }

  it should "flatten an array of objects with parent<idx>.field keys when flatten=true" in {
    // The recursion uses 1-based indexing: first array element gets "1", second "2".
    val node = parse("""{"items":[{"id":"a"},{"id":"b","extra":"e"}]}""")
    JSONUtils.JSONToMap(node, flatten = true) shouldBe Map(
      "items1.id" -> "a",
      "items2.id" -> "b",
      "items2.extra" -> "e"
    )
  }

  it should "drop array-of-primitive elements when flatten=true (current behavior)" in {
    // Pin: the docstring claims `{"E":["X","Y"]}` flattens to
    // `{"E1":"X","E2":"Y"}`, but the implementation only emits an entry when
    // the recursive call is iterating an *object* node. Recursing into a
    // value node returns an empty map, so primitives inside an array are
    // silently dropped. Document this divergence so a future fix that
    // brings the code into line with the docstring will deliberately
    // break this spec and force the contract to be reviewed together.
    val node = parse("""{"a":"x","arr":["X","Y"]}""")
    JSONUtils.JSONToMap(node, flatten = true) shouldBe Map("a" -> "x")
  }

  it should "respect an explicit parentName for keying" in {
    val node = parse("""{"k":"v"}""")
    JSONUtils.JSONToMap(node, flatten = false, parentName = "outer") shouldBe Map(
      "outer.k" -> "v"
    )
  }

  it should "return an empty map for a top-level value node" in {
    // Only object nodes contribute entries directly; a bare value at the top
    // level (e.g. raw JSON `42`) has no parent key to attach to.
    JSONUtils.JSONToMap(parse("42")) shouldBe Map.empty[String, String]
    JSONUtils.JSONToMap(parse("\"x\"")) shouldBe Map.empty[String, String]
    JSONUtils.JSONToMap(parse("null")) shouldBe Map.empty[String, String]
  }

  it should "return an empty map for a top-level array even when flatten=true" in {
    // A top-level array is iterated with parentName="" so children become
    // "1", "2", ...; primitives inside still produce no entries (same root
    // cause as the array-of-primitives case above), and a top-level array
    // therefore yields nothing for primitive content.
    JSONUtils.JSONToMap(parse("[1,2,3]"), flatten = true) shouldBe Map.empty[String, String]
  }

  it should "key a top-level array of objects with the bare 1-based index" in {
    val node = parse("""[{"id":"a"},{"id":"b"}]""")
    JSONUtils.JSONToMap(node, flatten = true) shouldBe Map(
      "1.id" -> "a",
      "2.id" -> "b"
    )
  }

  // ----- objectMapper configuration -----

  "objectMapper" should "exclude null and absent fields from serialized output" in {
    case class Box(present: String, opt: Option[String])
    val box = Box("kept", None)
    // Parse back into a JsonNode so the assertion is structural rather than
    // substring-based: pretty-printing changes or a "kept" value that happens
    // to contain "opt" would otherwise produce false positives/negatives.
    val root = parse(JSONUtils.objectMapper.writeValueAsString(box))
    root.get("present").asText() shouldBe "kept"
    root.has("opt") shouldBe false
  }

  it should "serialize Scala collections via DefaultScalaModule" in {
    // Without DefaultScalaModule registration, Scala Seqs / Maps fall through
    // to Jackson's bean serialization and emit reflection-leaking output. Pin
    // the working contract so a future ObjectMapper rewire that drops the
    // module catches the regression here. Use structural assertions on a
    // parsed tree so whitespace / pretty-printing changes don't flake.
    val payload = Map("xs" -> Seq("a", "b"), "n" -> Seq.empty[String])
    val root = parse(JSONUtils.objectMapper.writeValueAsString(payload))

    val xs = root.get("xs")
    xs.isArray shouldBe true
    xs.size() shouldBe 2
    xs.get(0).asText() shouldBe "a"
    xs.get(1).asText() shouldBe "b"

    val n = root.get("n")
    n.isArray shouldBe true
    n.size() shouldBe 0
  }
}
