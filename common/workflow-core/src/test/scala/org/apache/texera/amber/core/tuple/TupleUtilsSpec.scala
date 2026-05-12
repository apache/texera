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

package org.apache.texera.amber.core.tuple

import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

class TupleUtilsSpec extends AnyFlatSpec {

  // --- tuple2json ------------------------------------------------------------

  "TupleUtils.tuple2json" should "emit one JSON field per schema attribute, in the schema's declared order" in {
    val schema = new Schema(
      new Attribute("id", AttributeType.INTEGER),
      new Attribute("name", AttributeType.STRING)
    )
    val node = TupleUtils.tuple2json(schema, Array[Any](Int.box(7), "alice"))
    // Field iteration order on Jackson ObjectNode follows insertion order,
    // which mirrors the schema's getAttributeNames order.
    assert(node.fieldNames().asScala.toList == List("id", "name"))
    assert(node.get("id").asInt() == 7)
    assert(node.get("name").asText() == "alice")
  }

  it should "emit JSON null for null field values" in {
    val schema = new Schema(new Attribute("v", AttributeType.STRING))
    val node = TupleUtils.tuple2json(schema, Array[Any](null))
    assert(node.get("v").isNull)
  }

  it should "respect schema.getIndex when fieldVals is laid out positionally" in {
    // Re-ordering the schema must change which slot of fieldVals each
    // attribute pulls from, because tuple2json indexes fieldVals via
    // schema.getIndex(attrName).
    val schema = new Schema(
      new Attribute("b", AttributeType.STRING),
      new Attribute("a", AttributeType.STRING)
    )
    val node = TupleUtils.tuple2json(schema, Array[Any]("first", "second"))
    assert(node.get("b").asText() == "first")
    assert(node.get("a").asText() == "second")
  }

  it should "produce an empty object for an empty schema" in {
    val node = TupleUtils.tuple2json(new Schema(), Array.empty[Any])
    assert(node.size() == 0)
  }

  // --- json2tuple ------------------------------------------------------------

  "TupleUtils.json2tuple" should "infer a schema from a flat JSON object's keys and types" in {
    val tuple = TupleUtils.json2tuple("""{"name": "bob", "age": 30}""")
    val names = tuple.getSchema.getAttributeNames.toSet
    assert(names == Set("name", "age"))
    assert(tuple.getField[Any]("name") == "bob")
    // age is parsed via inferSchemaFromRows; the inferred type for "30" is
    // a numeric type — assert we can read the field rather than locking in
    // the precise inferred AttributeType.
    assert(tuple.getField[Any]("age").toString == "30")
  }

  it should "round-trip a schema-and-values through tuple2json → json2tuple" in {
    val schema = new Schema(
      new Attribute("city", AttributeType.STRING),
      new Attribute("score", AttributeType.INTEGER)
    )
    val original = TupleUtils.tuple2json(schema, Array[Any]("Irvine", Int.box(42))).toString
    val parsed = TupleUtils.json2tuple(original)
    val reSerialized =
      TupleUtils.tuple2json(parsed.getSchema, parsed.getFields.toArray.asInstanceOf[Array[Any]])
    // The exact column order isn't part of the json2tuple contract (it builds
    // schemaFieldNames from a Set), so compare by JSON-tree equality.
    val mapper = org.apache.texera.amber.util.JSONUtils.objectMapper
    assert(mapper.readTree(reSerialized.toString) == mapper.readTree(original))
  }

  it should "drop non-object roots (e.g. a JSON array) into an empty tuple" in {
    // The implementation only collects fields when the root `isObject`. A
    // non-object root leaves `fieldNames` empty, so the result is a tuple
    // over an empty schema with no fields — observed contract is no-throw,
    // empty result.
    val tuple = TupleUtils.json2tuple("""[1, 2, 3]""")
    assert(tuple.getSchema.getAttributes.isEmpty)
    assert(tuple.getFields.isEmpty)
  }

  it should "throw when given malformed JSON" in {
    intercept[Exception] {
      TupleUtils.json2tuple("{ this is not json }")
    }
  }
}
