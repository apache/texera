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

package edu.uci.ics.amber.core.tuple

import org.scalatest.funspec.AnyFunSpec
import java.sql.Timestamp
import java.util.List

class AttributeTypeSpec extends AnyFunSpec {

  describe("AttributeType.getAttributeType") {

    it("should return STRING for String class when isLargeBinary is false") {
      assert(
        AttributeType.getAttributeType(
          classOf[String],
          false
        ) === AttributeType.STRING
      )
    }

    it("should return LARGE_BINARY for String class when isLargeBinary is true") {
      assert(
        AttributeType.getAttributeType(
          classOf[String],
          true
        ) === AttributeType.LARGE_BINARY
      )
    }

    it("should return INTEGER for Integer class") {
      assert(
        AttributeType.getAttributeType(
          classOf[Integer],
          false
        ) === AttributeType.INTEGER
      )
    }

    it("should return LONG for Long class") {
      assert(
        AttributeType.getAttributeType(classOf[java.lang.Long], false) === AttributeType.LONG
      )
    }

    it("should return DOUBLE for Double class") {
      assert(
        AttributeType.getAttributeType(
          classOf[java.lang.Double],
          false
        ) === AttributeType.DOUBLE
      )
    }

    it("should return BOOLEAN for Boolean class") {
      assert(
        AttributeType.getAttributeType(
          classOf[java.lang.Boolean],
          false
        ) === AttributeType.BOOLEAN
      )
    }

    it("should return TIMESTAMP for Timestamp class") {
      assert(
        AttributeType.getAttributeType(
          classOf[Timestamp],
          false
        ) === AttributeType.TIMESTAMP
      )
    }

    it("should return BINARY for byte array class") {
      assert(
        AttributeType.getAttributeType(
          classOf[Array[Byte]],
          false
        ) === AttributeType.BINARY
      )
    }

    it("should return ANY for unknown types") {
      assert(
        AttributeType.getAttributeType(classOf[Object], false) === AttributeType.ANY
      )
      assert(
        AttributeType.getAttributeType(
          classOf[List[_]],
          false
        ) === AttributeType.ANY
      )
    }
  }

  describe("AttributeType.validateAttributeName") {
    it("should accept valid attribute names") {
      // Valid attribute names should not throw any exception
      AttributeType.validateAttributeName("valid_name")
      AttributeType.validateAttributeName("another_valid_name")
      AttributeType.validateAttributeName("name_with_numbers_123")
    }

    it("should throw IllegalArgumentException for attribute names starting with reserved prefix") {
      val reservedPrefix = AttributeType.TEXERA_LARGE_BINARY_TYPE_ATTRIBUTE_NAME_PREFIX

      // Test with exact prefix
      val exception1 = intercept[IllegalArgumentException] {
        AttributeType.validateAttributeName(reservedPrefix)
      }
      assert(exception1.getMessage.contains("Attribute name cannot start with the reserved prefix"))

      // Test with prefix followed by additional text
      val exception2 = intercept[IllegalArgumentException] {
        AttributeType.validateAttributeName(reservedPrefix + "some_text")
      }
      assert(exception2.getMessage.contains("Attribute name cannot start with the reserved prefix"))
    }
  }
}
