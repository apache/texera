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

package org.apache.texera.amber.operator.filter

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ComparisonTypeSpec extends AnyFlatSpec with Matchers {

  "ComparisonType" should "map each constant to its wire symbol via getName" in {
    ComparisonType.EQUAL_TO.getName shouldBe "="
    ComparisonType.GREATER_THAN.getName shouldBe ">"
    ComparisonType.GREATER_THAN_OR_EQUAL_TO.getName shouldBe ">="
    ComparisonType.LESS_THAN.getName shouldBe "<"
    ComparisonType.LESS_THAN_OR_EQUAL_TO.getName shouldBe "<="
    ComparisonType.NOT_EQUAL_TO.getName shouldBe "!="
    ComparisonType.IS_NULL.getName shouldBe "is null"
    ComparisonType.IS_NOT_NULL.getName shouldBe "is not null"
    ComparisonType.values() should have length 8
  }

  "ComparisonType.fromString" should "resolve symbols case-insensitively" in {
    ComparisonType.fromString(">=") shouldBe ComparisonType.GREATER_THAN_OR_EQUAL_TO
    ComparisonType.fromString("IS NULL") shouldBe ComparisonType.IS_NULL
    ComparisonType.fromString("is not null") shouldBe ComparisonType.IS_NOT_NULL
  }

  it should "reject an unknown symbol" in {
    intercept[IllegalArgumentException](ComparisonType.fromString("≈"))
  }

  "ComparisonType" should "round-trip through Jackson using its symbol" in {
    objectMapper.writeValueAsString(ComparisonType.GREATER_THAN) shouldBe "\">\""
    objectMapper.readValue("\"is null\"", classOf[ComparisonType]) shouldBe ComparisonType.IS_NULL
  }
}
