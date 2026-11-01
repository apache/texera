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
}
