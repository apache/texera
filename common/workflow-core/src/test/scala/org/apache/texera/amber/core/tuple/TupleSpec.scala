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

import org.apache.texera.amber.core.tuple.TupleUtils.{json2tuple, tuple2json}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp

class TupleSpec extends AnyFlatSpec {
  val stringAttribute = new Attribute("col-string", AttributeType.STRING)
  val integerAttribute = new Attribute("col-int", AttributeType.INTEGER)
  val boolAttribute = new Attribute("col-bool", AttributeType.BOOLEAN)
  val longAttribute = new Attribute("col-long", AttributeType.LONG)
  val doubleAttribute = new Attribute("col-double", AttributeType.DOUBLE)
  val timestampAttribute = new Attribute("col-timestamp", AttributeType.TIMESTAMP)
  val binaryAttribute = new Attribute("col-binary", AttributeType.BINARY)

  val capitalizedStringAttribute = new Attribute("COL-string", AttributeType.STRING)

  it should "create a tuple with capitalized attributeName" in {

    val schema = Schema().add(capitalizedStringAttribute)
    val tuple = Tuple.builder(schema).add(capitalizedStringAttribute, "string-value").build()
    assert(tuple.getField("COL-string").asInstanceOf[String] == "string-value")

  }

  it should "create a tuple with capitalized attributeName, using addSequentially" in {
    val schema = Schema().add(capitalizedStringAttribute)
    val tuple = Tuple.builder(schema).addSequentially(Array("string-value")).build()
    assert(tuple.getField("COL-string").asInstanceOf[String] == "string-value")
  }

  it should "create a tuple using new builder, based on another tuple using old builder" in {
    val schema = Schema().add(stringAttribute)
    val inputTuple = Tuple.builder(schema).addSequentially(Array("string-value")).build()
    val newTuple = Tuple.builder(inputTuple.getSchema).add(inputTuple).build()

    assert(newTuple.length == inputTuple.length)
  }

  it should "fail when unknown attribute is added to tuple" in {
    val schema = Schema().add(stringAttribute)
    assertThrows[TupleBuildingException] {
      Tuple.builder(schema).add(integerAttribute, 1)
    }
  }

  it should "fail when tuple does not conform to complete schema" in {
    val schema = Schema().add(stringAttribute).add(integerAttribute)
    assertThrows[TupleBuildingException] {
      Tuple.builder(schema).add(integerAttribute, 1).build()
    }
  }

  it should "fail when entire tuple passed in has extra attributes" in {
    val inputSchema = Schema().add(stringAttribute).add(integerAttribute).add(boolAttribute)
    val inputTuple = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .build()

    val outputSchema = Schema().add(stringAttribute).add(integerAttribute)
    assertThrows[TupleBuildingException] {
      Tuple.builder(outputSchema).add(inputTuple).build()
    }
  }

  it should "not fail when entire tuple passed in has extra attributes and strictSchemaMatch is false" in {
    val inputSchema =
      Schema().add(stringAttribute).add(integerAttribute).add(boolAttribute)
    val inputTuple = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .build()

    val outputSchema = Schema().add(stringAttribute).add(integerAttribute)
    val outputTuple = Tuple.builder(outputSchema).add(inputTuple, false).build()

    // This is the important test. Input tuple has 3 attributes but output tuple has only 2
    // It's because of isStrictSchemaMatch=false
    assert(outputTuple.length == 2);
  }

  it should "produce identical strings" in {
    val inputSchema =
      Schema().add(stringAttribute).add(integerAttribute).add(boolAttribute)
    val inputTuple = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 1)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .build()

    val line = tuple2json(inputTuple.schema, inputTuple.fieldVals).toString
    val newTuple = json2tuple(line)
    assert(line == tuple2json(newTuple.schema, newTuple.fieldVals).toString)

  }

  it should "calculate hash" in {
    val inputSchema =
      Schema()
        .add(integerAttribute)
        .add(stringAttribute)
        .add(boolAttribute)
        .add(longAttribute)
        .add(doubleAttribute)
        .add(timestampAttribute)
        .add(binaryAttribute)

    val inputTuple = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 922323)
      .add(stringAttribute, "string-attr")
      .add(boolAttribute, true)
      .add(longAttribute, 1123213213213L)
      .add(doubleAttribute, 214214.9969346)
      .add(timestampAttribute, new Timestamp(100000000L))
      .add(binaryAttribute, Array[Byte](104, 101, 108, 108, 111))
      .build()
    assert(inputTuple.hashCode() == -1335416166)

    val inputTuple2 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, 0)
      .add(stringAttribute, "")
      .add(boolAttribute, false)
      .add(longAttribute, 0L)
      .add(doubleAttribute, 0.0)
      .add(timestampAttribute, new Timestamp(0L))
      .add(binaryAttribute, Array[Byte]())
      .build()
    assert(inputTuple2.hashCode() == -1409761483)

    val inputTuple3 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, null)
      .add(stringAttribute, null)
      .add(boolAttribute, null)
      .add(longAttribute, null)
      .add(doubleAttribute, null)
      .add(timestampAttribute, null)
      .add(binaryAttribute, null)
      .build()
    assert(inputTuple3.hashCode() == 1742810335)

    val inputTuple4 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, -3245763)
      .add(stringAttribute, "\n\r\napple")
      .add(boolAttribute, true)
      .add(longAttribute, -8965536434247L)
      .add(doubleAttribute, 1 / 3.0d)
      .add(timestampAttribute, new Timestamp(-1990))
      .add(binaryAttribute, null)
      .build()
    assert(inputTuple4.hashCode() == -592643630)

    val inputTuple5 = Tuple
      .builder(inputSchema)
      .add(integerAttribute, Int.MaxValue)
      .add(stringAttribute, new String())
      .add(boolAttribute, true)
      .add(longAttribute, Long.MaxValue)
      .add(doubleAttribute, 7 / 17.0d)
      .add(timestampAttribute, new Timestamp(1234567890L))
      .add(binaryAttribute, Array.fill[Byte](4097)('o'))
      .build()
    assert(inputTuple5.hashCode() == -2099556631)
  }

  it should "reject getField for an attribute name that is not in the tuple" in {
    val schema = Schema().add(stringAttribute)
    val tuple = Tuple.builder(schema).add(stringAttribute, "v").build()
    val ex = intercept[RuntimeException] {
      tuple.getField[String]("col-missing")
    }
    assert(ex.getMessage == "col-missing is not in the tuple")
  }

  it should "get a field by Attribute" in {
    val schema = Schema().add(stringAttribute)
    val tuple = Tuple.builder(schema).add(stringAttribute, "v").build()
    assert(tuple.getField[String](stringAttribute) == "v")
  }

  it should "enforce its own schema and reject a different one" in {
    val schema = Schema().add(stringAttribute)
    val tuple = Tuple.builder(schema).add(stringAttribute, "v").build()
    assert(tuple.enforceSchema(schema) eq tuple)
    val ex = intercept[AssertionError] {
      tuple.enforceSchema(Schema().add(integerAttribute))
    }
    assert(ex.getMessage.contains("output tuple schema does not match the expected schema!"))
  }

  it should "compare tuples by value, including binary contents" in {
    val schema = Schema().add(stringAttribute).add(binaryAttribute)
    def tupleWith(s: String): Tuple =
      Tuple
        .builder(schema)
        .add(stringAttribute, s)
        .add(binaryAttribute, Array[Byte](1, 2, 3))
        .build()
    val tupleA = tupleWith("v")
    val tupleB = tupleWith("v")
    assert(tupleA == tupleB) // distinct Array[Byte] instances, same contents
    val differentField = tupleWith("other")
    assert(tupleA != differentField)
    val differentSchema =
      Tuple.builder(Schema().add(stringAttribute)).add(stringAttribute, "v").build()
    assert(tupleA != differentSchema)
    assert(!tupleA.equals("not a tuple"))
    assert(!tupleA.equals(null))
  }

  it should "build a partial tuple following the requested attribute order" in {
    val schema = Schema().add(stringAttribute).add(integerAttribute).add(boolAttribute)
    val tuple = Tuple
      .builder(schema)
      .add(stringAttribute, "s")
      .add(integerAttribute, 1)
      .add(boolAttribute, true)
      .build()
    val partial = tuple.getPartialTuple(List("col-bool", "col-string"))
    assert(partial.length == 2)
    assert(partial.getField[Boolean]("col-bool"))
    assert(partial.getField[String]("col-string") == "s")
    assert(partial.getSchema == Schema().add(boolAttribute).add(stringAttribute))
  }

  it should "render toString with the schema and field values" in {
    val schema = Schema().add(stringAttribute)
    val tuple = Tuple.builder(schema).add(stringAttribute, "v").build()
    assert(tuple.toString == s"Tuple [schema=$schema, fields=[v]]")
  }

  it should "reject construction when schema and field sizes differ" in {
    val schema = Schema().add(stringAttribute)
    val ex = intercept[RuntimeException] {
      Tuple(schema, Array[Any]("a", "b"))
    }
    assert(ex.getMessage == "Schema size (1) and field size (2) are different")
  }

  it should "reject a field whose class does not match the attribute type" in {
    val ex = intercept[RuntimeException] {
      Tuple.builder(Schema().add(stringAttribute)).add(stringAttribute, Integer.valueOf(1))
    }
    assert(
      ex.getMessage ==
        "Attribute col-string's type (string) is different from field's type (integer)"
    )
  }

  it should "accept any field class for an ANY-typed attribute" in {
    val anyAttribute = new Attribute("col-any", AttributeType.ANY)
    val tuple =
      Tuple.builder(Schema().add(anyAttribute)).add(anyAttribute, List(1, 2, 3)).build()
    assert(tuple.getField[List[Int]]("col-any") == List(1, 2, 3))
  }

  it should "support the three-argument builder add and reject null arguments" in {
    val schema = Schema().add(stringAttribute)
    val tuple =
      Tuple.builder(schema).add("col-string", AttributeType.STRING, "v").build()
    assert(tuple.getField[String]("col-string") == "v")
    intercept[IllegalArgumentException] {
      Tuple.builder(schema).add(null.asInstanceOf[String], null, "v")
    }
    intercept[IllegalArgumentException] {
      Tuple.builder(schema).add(null.asInstanceOf[Attribute], "v")
    }
    intercept[IllegalArgumentException] {
      Tuple.builder(schema).add(null.asInstanceOf[Tuple])
    }
    intercept[IllegalArgumentException] {
      Tuple.builder(schema).addSequentially(null)
    }
  }

  it should "expose the case-class members of Tuple" in {
    val schema = Schema().add(stringAttribute)
    val tuple = Tuple(schema, Array[Any]("hello"))
    val copied = tuple.copy()
    assert(copied == tuple)
    assert(tuple.canEqual(copied))
    assert(tuple.productArity == 2)
    assert(tuple.productElement(0) == schema)
    val Tuple(unappliedSchema, _) = tuple
    assert(unappliedSchema == schema)
  }
}
