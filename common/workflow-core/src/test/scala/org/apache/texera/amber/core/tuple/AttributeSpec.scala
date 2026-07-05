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

class AttributeSpec extends AnyFlatSpec {

  "Attribute" should "expose its name and type" in {
    val attribute = new Attribute("age", AttributeType.INTEGER)
    assert(attribute.getName == "age")
    assert(attribute.getType == AttributeType.INTEGER)
  }

  // AttributeType's own semantics (wire names, field-class mapping) are covered by AttributeTypeSpec;
  // here we just confirm the Attribute holder carries every type faithfully.
  it should "carry the name and every attribute type" in {
    AttributeType.values().foreach { attributeType =>
      val attribute = new Attribute("col", attributeType)
      assert(attribute.getName == "col")
      assert(attribute.getType == attributeType)
    }
  }

  it should "reject null constructor arguments" in {
    assertThrows[NullPointerException](new Attribute(null, AttributeType.STRING))
    assertThrows[NullPointerException](new Attribute("x", null))
  }

  it should "render its type via the lowercase wire name in toString" in {
    assert(
      new Attribute("name", AttributeType.STRING).toString == "Attribute[name=name, type=string]"
    )
    assert(
      new Attribute("age", AttributeType.INTEGER).toString == "Attribute[name=age, type=integer]"
    )
  }

  "Attribute.equals" should "cover identity, null, other-type, and field comparisons" in {
    val attribute = new Attribute("age", AttributeType.INTEGER)
    assert(attribute.equals(attribute)) // identity
    assert(!attribute.equals(null)) // null argument
    assert(!attribute.equals("age")) // different class
    assert(attribute.equals(new Attribute("age", AttributeType.INTEGER))) // exact match
    assert(
      attribute.equals(new Attribute("AGE", AttributeType.INTEGER))
    ) // name is case-insensitive
    assert(!attribute.equals(new Attribute("name", AttributeType.INTEGER))) // different name
    assert(!attribute.equals(new Attribute("age", AttributeType.STRING))) // different type
  }

  "Attribute.hashCode" should "combine the name hash and the type wire-name hash" in {
    assert(
      new Attribute("age", AttributeType.INTEGER).hashCode ==
        new Attribute("age", AttributeType.INTEGER).hashCode
    )
    assert(
      new Attribute("age", AttributeType.INTEGER).hashCode == "age".hashCode + "integer".hashCode
    )
    // documented quirk: equals is case-insensitive on the name but hashCode is case-sensitive,
    // so equal-by-equals attributes with differently-cased names have different hash codes
    assert(
      new Attribute("age", AttributeType.INTEGER)
        .equals(new Attribute("AGE", AttributeType.INTEGER))
    )
    assert(
      new Attribute("age", AttributeType.INTEGER).hashCode !=
        new Attribute("AGE", AttributeType.INTEGER).hashCode
    )
  }
}
