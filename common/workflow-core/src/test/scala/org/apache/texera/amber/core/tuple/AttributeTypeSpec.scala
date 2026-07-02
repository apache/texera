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

class AttributeTypeSpec extends AnyFlatSpec {

  // ----- getName -----

  "AttributeType.getName" should "return the lowercase wire name for each concrete type" in {
    assert(AttributeType.STRING.getName == "string")
    assert(AttributeType.INTEGER.getName == "integer")
    assert(AttributeType.LONG.getName == "long")
    assert(AttributeType.DOUBLE.getName == "double")
    assert(AttributeType.BOOLEAN.getName == "boolean")
    assert(AttributeType.TIMESTAMP.getName == "timestamp")
    assert(AttributeType.BINARY.getName == "binary")
    assert(AttributeType.LARGE_BINARY.getName == "large_binary")
  }

  it should "return an empty string for ANY (excluded from the JSON schema)" in {
    assert(AttributeType.ANY.getName == "")
    assert(AttributeType.ANY.toString == "")
    assert(AttributeType.ANY.name() == "ANY") // the built-in enum name is unaffected
  }

  // ----- getAttributeType -----

  "AttributeType.getAttributeType" should "map each supported field class to its type" in {
    assert(AttributeType.getAttributeType(classOf[String]) == AttributeType.STRING)
    assert(AttributeType.getAttributeType(classOf[java.lang.Integer]) == AttributeType.INTEGER)
    assert(AttributeType.getAttributeType(classOf[java.lang.Long]) == AttributeType.LONG)
    assert(AttributeType.getAttributeType(classOf[java.lang.Double]) == AttributeType.DOUBLE)
    assert(AttributeType.getAttributeType(classOf[java.lang.Boolean]) == AttributeType.BOOLEAN)
    assert(AttributeType.getAttributeType(classOf[java.sql.Timestamp]) == AttributeType.TIMESTAMP)
    assert(AttributeType.getAttributeType(classOf[Array[Byte]]) == AttributeType.BINARY)
    assert(AttributeType.getAttributeType(classOf[LargeBinary]) == AttributeType.LARGE_BINARY)
  }

  it should "fall back to ANY for unrecognized classes, including primitives" in {
    assert(AttributeType.getAttributeType(classOf[Object]) == AttributeType.ANY)
    // Scala's classOf[Int] is the primitive int.class, which the boxed-class
    // checks do not match — it falls through to ANY.
    assert(AttributeType.getAttributeType(classOf[Int]) == AttributeType.ANY)
  }

  it should "round-trip every concrete type through its field class" in {
    AttributeType
      .values()
      .filter(_ != AttributeType.ANY)
      .foreach(t => assert(AttributeType.getAttributeType(t.getFieldClass) == t))
  }
}
