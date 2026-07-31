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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class FilterPredicateSpec extends AnyFlatSpec with Matchers {

  private val intSchema = Schema().add(new Attribute("age", AttributeType.INTEGER))
  private def ageTuple(age: Integer): Tuple =
    Tuple.builder(intSchema).add("age", AttributeType.INTEGER, age).build()

  "FilterPredicate" should "expose its constructor-supplied fields" in {
    val p = new FilterPredicate("age", ComparisonType.GREATER_THAN, "18")
    p.attribute shouldBe "age"
    p.condition shouldBe ComparisonType.GREATER_THAN
    p.value shouldBe "18"
  }

  "FilterPredicate.evaluate" should "apply numeric comparisons against the tuple field" in {
    new FilterPredicate("age", ComparisonType.GREATER_THAN, "18")
      .evaluate(ageTuple(30)) shouldBe true
    new FilterPredicate("age", ComparisonType.GREATER_THAN, "18")
      .evaluate(ageTuple(10)) shouldBe false
    new FilterPredicate("age", ComparisonType.LESS_THAN_OR_EQUAL_TO, "18")
      .evaluate(ageTuple(18)) shouldBe true
  }

  it should "handle the null-check conditions without parsing the value" in {
    new FilterPredicate("age", ComparisonType.IS_NOT_NULL, null).evaluate(ageTuple(5)) shouldBe true
  }

  it should "compare string fields when the value is non-numeric" in {
    val schema = Schema().add(new Attribute("name", AttributeType.STRING))
    val t = Tuple.builder(schema).add("name", AttributeType.STRING, "bob").build()
    new FilterPredicate("name", ComparisonType.EQUAL_TO, "bob").evaluate(t) shouldBe true
    new FilterPredicate("name", ComparisonType.NOT_EQUAL_TO, "bob").evaluate(t) shouldBe false
  }

  "FilterPredicate" should "honor equals/hashCode by its three fields" in {
    val a = new FilterPredicate("age", ComparisonType.EQUAL_TO, "1")
    val b = new FilterPredicate("age", ComparisonType.EQUAL_TO, "1")
    val c = new FilterPredicate("age", ComparisonType.EQUAL_TO, "2")
    a shouldBe b
    a.hashCode shouldBe b.hashCode
    a should not be c
  }

  "FilterPredicate" should "round-trip through Jackson (condition as its symbol)" in {
    val p = new FilterPredicate("age", ComparisonType.GREATER_THAN_OR_EQUAL_TO, "18")
    val json = objectMapper.writeValueAsString(p)
    val node = objectMapper.readTree(json)
    node.get("attribute").asText shouldBe "age"
    node.get("condition").asText shouldBe ">="
    node.get("value").asText shouldBe "18"
    objectMapper.readValue(json, classOf[FilterPredicate]) shouldBe p
  }

  private def singleFieldTuple(attributeType: AttributeType, field: Any): Tuple = {
    val schema = Schema().add(new Attribute("col", attributeType))
    Tuple.builder(schema).add("col", attributeType, field).build()
  }

  "FilterPredicate.evaluate" should "apply the remaining comparison operators" in {
    new FilterPredicate("age", ComparisonType.GREATER_THAN_OR_EQUAL_TO, "18")
      .evaluate(ageTuple(18)) shouldBe true
    new FilterPredicate("age", ComparisonType.GREATER_THAN_OR_EQUAL_TO, "18")
      .evaluate(ageTuple(17)) shouldBe false
    new FilterPredicate("age", ComparisonType.LESS_THAN, "18").evaluate(ageTuple(10)) shouldBe true
    new FilterPredicate("age", ComparisonType.EQUAL_TO, "18").evaluate(ageTuple(18)) shouldBe true
  }

  it should "return false for non-null-check conditions when the field is null" in {
    new FilterPredicate("age", ComparisonType.EQUAL_TO, "1").evaluate(ageTuple(null)) shouldBe false
  }

  it should "evaluate IS_NULL against null and non-null fields" in {
    new FilterPredicate("age", ComparisonType.IS_NULL, null).evaluate(ageTuple(null)) shouldBe true
    new FilterPredicate("age", ComparisonType.IS_NULL, null).evaluate(ageTuple(5)) shouldBe false
    new FilterPredicate("age", ComparisonType.IS_NOT_NULL, null)
      .evaluate(ageTuple(null)) shouldBe false
  }

  it should "compare boolean fields case-insensitively against the value" in {
    val t = singleFieldTuple(AttributeType.BOOLEAN, java.lang.Boolean.TRUE)
    new FilterPredicate("col", ComparisonType.EQUAL_TO, "TRUE").evaluate(t) shouldBe true
    new FilterPredicate("col", ComparisonType.EQUAL_TO, "false").evaluate(t) shouldBe false
    new FilterPredicate("col", ComparisonType.NOT_EQUAL_TO, "false").evaluate(t) shouldBe true
  }

  it should "compare double fields numerically" in {
    val t = singleFieldTuple(AttributeType.DOUBLE, java.lang.Double.valueOf(3.5))
    new FilterPredicate("col", ComparisonType.GREATER_THAN, "3.0").evaluate(t) shouldBe true
    new FilterPredicate("col", ComparisonType.LESS_THAN_OR_EQUAL_TO, "3.5")
      .evaluate(t) shouldBe true
  }

  it should "compare long fields numerically" in {
    val t = singleFieldTuple(AttributeType.LONG, java.lang.Long.valueOf(100L))
    new FilterPredicate("col", ComparisonType.LESS_THAN_OR_EQUAL_TO, "100")
      .evaluate(t) shouldBe true
    new FilterPredicate("col", ComparisonType.GREATER_THAN, "99").evaluate(t) shouldBe true
  }

  it should "compare timestamp fields against parsed timestamp values" in {
    val t =
      singleFieldTuple(AttributeType.TIMESTAMP, Timestamp.valueOf("2020-01-01 00:00:00"))
    new FilterPredicate("col", ComparisonType.EQUAL_TO, "2020-01-01 00:00:00")
      .evaluate(t) shouldBe true
    new FilterPredicate("col", ComparisonType.GREATER_THAN, "2019-01-01 00:00:00")
      .evaluate(t) shouldBe true
  }

  it should "compare numeric strings numerically before falling back to string comparison" in {
    val t = singleFieldTuple(AttributeType.STRING, "10")
    // string comparison would put "10" before "9"; the numeric path compares 10 > 9
    new FilterPredicate("col", ComparisonType.GREATER_THAN, "9").evaluate(t) shouldBe true
  }

  "FilterPredicate.equals" should "treat the same instance as equal and reject other types" in {
    val p = new FilterPredicate("age", ComparisonType.EQUAL_TO, "1")
    p.equals(p) shouldBe true
    p.equals(null) shouldBe false
    p.equals("not a predicate") shouldBe false
  }

  // --- attribute types that no other test reaches ----------------------------

  it should "route ANY columns through the string comparison path" in {
    // ANY shares the STRING case of the type switch: the field is stringified and
    // then compared numerically when both sides parse.
    val t = singleFieldTuple(AttributeType.ANY, java.lang.Integer.valueOf(42))
    new FilterPredicate("col", ComparisonType.GREATER_THAN, "9").evaluate(t) shouldBe true
    new FilterPredicate("col", ComparisonType.EQUAL_TO, "42").evaluate(t) shouldBe true
    new FilterPredicate("col", ComparisonType.LESS_THAN, "9").evaluate(t) shouldBe false
  }

  it should "fall back to lexicographic comparison when an ANY field is not numeric" in {
    val t = singleFieldTuple(AttributeType.ANY, java.lang.Boolean.TRUE)
    new FilterPredicate("col", ComparisonType.EQUAL_TO, "true").evaluate(t) shouldBe true
    new FilterPredicate("col", ComparisonType.NOT_EQUAL_TO, "false").evaluate(t) shouldBe true
  }

  it should "reject attribute types it cannot compare" in {
    val t = singleFieldTuple(AttributeType.BINARY, Array[Byte](1, 2, 3))
    val ex = intercept[RuntimeException] {
      new FilterPredicate("col", ComparisonType.EQUAL_TO, "1").evaluate(t)
    }
    ex.getMessage shouldBe "unsupported attribute type: binary"
  }

  it should "still answer the null checks on an otherwise unsupported type" in {
    // IS_NULL / IS_NOT_NULL short-circuit before the type switch, so a BINARY
    // column is filterable for nullness even though it cannot be compared.
    val t = singleFieldTuple(AttributeType.BINARY, Array[Byte](1))
    new FilterPredicate("col", ComparisonType.IS_NOT_NULL, null).evaluate(t) shouldBe true
    new FilterPredicate("col", ComparisonType.IS_NULL, null).evaluate(t) shouldBe false
  }

  // --- value-side parsing ----------------------------------------------------

  it should "compare a numeric string field lexicographically when the value is not numeric" in {
    // The tuple side parses as a number but the user-supplied value does not, so
    // the numeric attempt aborts and both sides are compared as text.
    val t = singleFieldTuple(AttributeType.STRING, "10")
    new FilterPredicate("col", ComparisonType.LESS_THAN, "abc").evaluate(t) shouldBe true
    new FilterPredicate("col", ComparisonType.EQUAL_TO, "abc").evaluate(t) shouldBe false
  }

  it should "propagate a parse failure when the value cannot be read as the column's type" in {
    val doubleTuple = singleFieldTuple(AttributeType.DOUBLE, java.lang.Double.valueOf(1.0))
    intercept[NumberFormatException] {
      new FilterPredicate("col", ComparisonType.EQUAL_TO, "not-a-number").evaluate(doubleTuple)
    }
    val longTuple = singleFieldTuple(AttributeType.LONG, java.lang.Long.valueOf(1L))
    intercept[NumberFormatException] {
      new FilterPredicate("col", ComparisonType.EQUAL_TO, "1.5").evaluate(longTuple)
    }
  }

  it should "trim surrounding whitespace off the value for boolean, long and timestamp columns" in {
    new FilterPredicate("col", ComparisonType.EQUAL_TO, "  TrUe  ")
      .evaluate(singleFieldTuple(AttributeType.BOOLEAN, java.lang.Boolean.TRUE)) shouldBe true
    new FilterPredicate("col", ComparisonType.EQUAL_TO, "  100  ")
      .evaluate(singleFieldTuple(AttributeType.LONG, java.lang.Long.valueOf(100L))) shouldBe true
    new FilterPredicate("col", ComparisonType.LESS_THAN, "  2021-01-01 00:00:00  ")
      .evaluate(
        singleFieldTuple(AttributeType.TIMESTAMP, Timestamp.valueOf("2020-01-01 00:00:00"))
      ) shouldBe true
  }
}
