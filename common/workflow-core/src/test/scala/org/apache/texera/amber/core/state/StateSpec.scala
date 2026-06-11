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

package org.apache.texera.amber.core.state

import org.scalatest.flatspec.AnyFlatSpec

class StateSpec extends AnyFlatSpec {

  "State" should "json-round-trip an empty state" in {
    val original = State(Map.empty)
    assert(State.fromJson(original.toJson) == original)
  }

  it should "json-round-trip primitive values" in {
    val original = State(
      Map(
        "string" -> "hello",
        "long" -> 42L,
        "double" -> 3.14,
        "bool_true" -> true,
        "bool_false" -> false
      )
    )
    val decoded = State.fromJson(original.toJson)
    assert(decoded.values("string") == "hello")
    assert(decoded.values("long") == 42L)
    assert(decoded.values("double") == 3.14)
    assert(decoded.values("bool_true") == true)
    assert(decoded.values("bool_false") == false)
  }

  it should "drop null entries during JSON serialization" in {
    // The shared `objectMapper` is configured with `Include.NON_NULL`, so
    // null values are stripped before they hit the wire. Document the
    // behavior here so callers know they cannot transport an explicit null
    // through a State -- Python's serializer keeps nulls but Scala does not.
    val original = State(Map("present" -> "value", "absent" -> null))
    val decoded = State.fromJson(original.toJson)
    assert(decoded.values.keySet == Set("present"))
    assert(decoded.values("present") == "value")
  }

  it should "json-round-trip byte arrays via the bytes type marker" in {
    val payload = Array[Byte](0, 1, 2, -1)
    val original = State(Map("payload" -> payload))
    val decoded = State.fromJson(original.toJson)
    val decodedBytes = decoded.values("payload").asInstanceOf[Array[Byte]]
    assert(decodedBytes.sameElements(payload))
  }

  it should "json-round-trip nested maps" in {
    val original = State(Map("outer" -> Map("inner" -> Map("value" -> 1L))))
    val decoded = State.fromJson(original.toJson)
    assert(decoded == original)
  }

  it should "json-round-trip lists of mixed values" in {
    val original = State(Map("items" -> List(1L, "two", 3.0, true, null)))
    val decoded = State.fromJson(original.toJson)
    assert(decoded == original)
  }

  it should "json-round-trip byte arrays nested inside lists and maps" in {
    val original = State(
      Map(
        "blobs" -> List(Array[Byte](1, 2), Array[Byte](3, 4)),
        "nested" -> Map("sub_blob" -> Array[Byte](5, 6))
      )
    )
    val decoded = State.fromJson(original.toJson)
    val blobs = decoded.values("blobs").asInstanceOf[List[Array[Byte]]]
    assert(blobs.head.sameElements(Array[Byte](1, 2)))
    assert(blobs(1).sameElements(Array[Byte](3, 4)))
    val subBlob = decoded.values
      .apply("nested")
      .asInstanceOf[Map[String, Any]]("sub_blob")
      .asInstanceOf[Array[Byte]]
    assert(subBlob.sameElements(Array[Byte](5, 6)))
  }

  it should "tuple-round-trip" in {
    val original = State(
      Map(
        "loop_counter" -> 3L,
        "label" -> "outer",
        "blob" -> Array[Byte](1, 2)
      )
    )
    val decoded = State.fromTuple(original.toTuple)
    assert(decoded.values("loop_counter") == 3L)
    assert(decoded.values("label") == "outer")
    assert(
      decoded.values("blob").asInstanceOf[Array[Byte]].sameElements(Array[Byte](1, 2))
    )
  }

  it should "produce a tuple whose payload is the JSON serialization" in {
    val tuple = State(Map("x" -> 1L)).toTuple
    assert(tuple.getSchema == State.schema)
    assert(tuple.getField[String]("content") == """{"x":1}""")
  }

  it should "decode a payload encoded by the Python serializer" in {
    // Wire-format compatibility check: the bytes-marker keys and the
    // single-row "content" column must match what core/models/state.py
    // emits, otherwise cross-language transport breaks.
    val pythonEmitted = """{"i":2,"blob":{"__texera_type__":"bytes","payload":"AQID"}}"""
    val decoded = State.fromJson(pythonEmitted)
    assert(decoded.values("i") == 2L)
    assert(
      decoded.values("blob").asInstanceOf[Array[Byte]].sameElements(Array[Byte](1, 2, 3))
    )
  }
}
