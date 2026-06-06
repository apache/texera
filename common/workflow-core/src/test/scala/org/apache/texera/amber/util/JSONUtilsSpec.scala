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
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, MissingNode}
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

  it should "flatten an array of primitives with parent<idx> keys when flatten=true" in {
    // Matches the docstring's worked example: `{"E":["X","Y"]}` flattens to
    // `{"E1":"X","E2":"Y"}` (parent name concatenated with the 1-based index,
    // no separator).
    val node = parse("""{"a":"x","arr":["X","Y"]}""")
    JSONUtils.JSONToMap(node, flatten = true) shouldBe Map(
      "a" -> "x",
      "arr1" -> "X",
      "arr2" -> "Y"
    )
  }

  it should "flatten a mixed array of objects and primitives when flatten=true" in {
    val node = parse("""{"mix":[{"id":"a"},"X",{"id":"b"}]}""")
    JSONUtils.JSONToMap(node, flatten = true) shouldBe Map(
      "mix1.id" -> "a",
      "mix2" -> "X",
      "mix3.id" -> "b"
    )
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

  it should "key a top-level array of primitives with the bare 1-based index" in {
    // A top-level array is iterated with parentName="" so each primitive
    // child is keyed by its 1-based index ("1", "2", ...).
    JSONUtils.JSONToMap(parse("[1,2,3]"), flatten = true) shouldBe Map(
      "1" -> "1",
      "2" -> "2",
      "3" -> "3"
    )
  }

  it should "key a top-level array of objects with the bare 1-based index" in {
    val node = parse("""[{"id":"a"},{"id":"b"}]""")
    JSONUtils.JSONToMap(node, flatten = true) shouldBe Map(
      "1.id" -> "a",
      "2.id" -> "b"
    )
  }

  it should "flatten nested arrays with concatenated 1-based index keys when flatten=true" in {
    // An array element that is itself an array is pushed back onto the worklist
    // and re-processed: the inner indices concatenate onto the outer parent with
    // no separator, so matrix[0][1] becomes "m12".
    val node = parse("""{"m":[[1,2],[3]]}""")
    JSONUtils.JSONToMap(node, flatten = true) shouldBe Map(
      "m11" -> "1",
      "m12" -> "2",
      "m21" -> "3"
    )
  }

  it should "render JSON null as the literal string \"null\" for nested fields when flatten=true" in {
    val node = parse("""{"outer":{"a":null}}""")
    JSONUtils.JSONToMap(node, flatten = true) shouldBe Map("outer.a" -> "null")
  }

  it should "contribute no entries for empty nested objects and arrays when flatten=true" in {
    // An empty object/array is pushed onto the worklist but yields nothing once
    // popped, so only the sibling primitive survives.
    val node = parse("""{"emptyObj":{},"emptyArr":[],"b":"x"}""")
    JSONUtils.JSONToMap(node, flatten = true) shouldBe Map("b" -> "x")
  }

  it should "return an empty map for a top-level empty array" in {
    JSONUtils.JSONToMap(parse("[]"), flatten = true) shouldBe Map.empty[String, String]
  }

  it should "ignore a node that is neither object, array, nor value node" in {
    // Defensive branch: a MissingNode is none of object/array/value, so the
    // traversal pops it and contributes nothing. Guards against a node type
    // that slips past all three predicates silently corrupting the result.
    JSONUtils.JSONToMap(MissingNode.getInstance()) shouldBe Map.empty[String, String]
  }

  it should "flatten very deeply nested JSON without overflowing the stack" in {
    // The traversal is iterative, so nesting depth lives on the heap rather than
    // the call stack: a depth that would StackOverflow a per-level recursion must
    // still produce the dotted leaf key. Build the tree programmatically rather
    // than via parse() so Jackson's own parser nesting limit doesn't cap the depth
    // before JSONToMap runs. Shape: {"a":{"a":{...{"leaf":"v"}...}}}.
    val depth = 20000
    var current = JsonNodeFactory.instance.objectNode()
    current.put("leaf", "v")
    for (_ <- 1 to depth) {
      val parent = JsonNodeFactory.instance.objectNode()
      parent.set[JsonNode]("a", current)
      current = parent
    }
    val expectedKey = ("a." * depth) + "leaf"
    JSONUtils.JSONToMap(current, flatten = true) shouldBe Map(expectedKey -> "v")
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
