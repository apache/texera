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

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class SchemaSpec extends AnyFlatSpec {

  private val allTypeSpecs: List[(String, AttributeType)] = List(
    "stringAttr" -> AttributeType.STRING,
    "integerAttr" -> AttributeType.INTEGER,
    "longAttr" -> AttributeType.LONG,
    "doubleAttr" -> AttributeType.DOUBLE,
    "booleanAttr" -> AttributeType.BOOLEAN,
    "timestampAttr" -> AttributeType.TIMESTAMP,
    "binaryAttr" -> AttributeType.BINARY
  )

  private val idNameSpecs: List[(String, AttributeType)] = List(
    "id" -> AttributeType.INTEGER,
    "name" -> AttributeType.STRING
  )

  private def attrs(specs: (String, AttributeType)*): List[Attribute] =
    specs.map { case (name, tpe) => new Attribute(name, tpe) }.toList

  private def schemaOf(specs: (String, AttributeType)*): Schema =
    Schema(attrs(specs: _*))

  "Schema" should "create an empty schema" in {
    val schema = Schema()
    assert(schema.getAttributes.isEmpty)
    assert(schema.getAttributeNames.isEmpty)
  }

  it should "create a schema with attributes of all types" in {
    val schema = schemaOf(allTypeSpecs: _*)
    assert(schema.getAttributes == attrs(allTypeSpecs: _*))
    assert(schema.getAttributeNames == allTypeSpecs.map(_._1))
  }

  it should "add a single attribute using add(Attribute)" in {
    val schema = Schema()
    val updatedSchema = schema.add(new Attribute("id", AttributeType.INTEGER))

    assert(updatedSchema.getAttributes == attrs("id" -> AttributeType.INTEGER))
  }

  it should "add multiple attributes using add(Attribute*)" in {
    val expected = attrs(
      "stringAttr" -> AttributeType.STRING,
      "integerAttr" -> AttributeType.INTEGER,
      "longAttr" -> AttributeType.LONG
    )
    val updatedSchema = Schema().add(expected: _*)

    assert(updatedSchema.getAttributes == expected)
  }

  it should "add attributes from another schema using add(Schema)" in {
    val schema1 = schemaOf("id" -> AttributeType.INTEGER)
    val schema2 = schemaOf("name" -> AttributeType.STRING)

    val mergedSchema = schema1.add(schema2)

    assert(mergedSchema.getAttributes == attrs(idNameSpecs: _*))
  }

  it should "add an attribute with name and type using add(String, AttributeType)" in {
    val updatedSchema = Schema().add("id", AttributeType.INTEGER)

    assert(updatedSchema.getAttributes == attrs("id" -> AttributeType.INTEGER))
  }

  it should "remove an existing attribute" in {
    val updatedSchema = schemaOf(idNameSpecs: _*).remove("id")

    assert(updatedSchema.getAttributes == attrs("name" -> AttributeType.STRING))
  }

  it should "throw an exception when removing a non-existent attribute" in {
    val schema = schemaOf("id" -> AttributeType.INTEGER)

    val exception = intercept[IllegalArgumentException] {
      schema.remove("name")
    }
    assert(exception.getMessage == "Cannot remove non-existent attributes: name")
  }

  it should "retrieve an attribute by name" in {
    val attribute = schemaOf(idNameSpecs: _*).getAttribute("id")

    assert(attribute == new Attribute("id", AttributeType.INTEGER))
  }

  it should "throw an exception when retrieving a non-existent attribute" in {
    val schema = schemaOf("id" -> AttributeType.INTEGER)

    val exception = intercept[RuntimeException] {
      schema.getAttribute("name")
    }
    assert(exception.getMessage == "name is not contained in the schema")
  }

  it should "return a partial schema for attributes of all types" in {
    val schema = schemaOf(
      "stringAttr" -> AttributeType.STRING,
      "integerAttr" -> AttributeType.INTEGER,
      "booleanAttr" -> AttributeType.BOOLEAN,
      "doubleAttr" -> AttributeType.DOUBLE
    )

    val partialSchema = schema.getPartialSchema(List("stringAttr", "booleanAttr"))

    assert(
      partialSchema.getAttributes == attrs(
        "stringAttr" -> AttributeType.STRING,
        "booleanAttr" -> AttributeType.BOOLEAN
      )
    )
  }

  it should "convert to raw schema and back for attributes of all types" in {
    val schema = schemaOf(allTypeSpecs: _*)

    val rawSchema = schema.toRawSchema
    assert(
      rawSchema == Map(
        "stringAttr" -> "STRING",
        "integerAttr" -> "INTEGER",
        "longAttr" -> "LONG",
        "doubleAttr" -> "DOUBLE",
        "booleanAttr" -> "BOOLEAN",
        "timestampAttr" -> "TIMESTAMP",
        "binaryAttr" -> "BINARY"
      )
    )

    val reconstructedSchema = Schema.fromRawSchema(rawSchema)
    assert(reconstructedSchema == schema)
  }

  it should "check if attributes exist in schema" in {
    val schema = schemaOf(
      "stringAttr" -> AttributeType.STRING,
      "integerAttr" -> AttributeType.INTEGER
    )

    assert(schema.containsAttribute("stringAttr"))
    assert(!schema.containsAttribute("nonExistentAttr"))
  }

  it should "return the index of an attribute by name" in {
    val schema = schemaOf(idNameSpecs: _*)

    assert(schema.getIndex("id") == 0)
    assert(schema.getIndex("name") == 1)
  }

  it should "throw an exception when getting the index of a non-existent attribute" in {
    val schema = schemaOf("id" -> AttributeType.INTEGER)

    val exception = intercept[RuntimeException] {
      schema.getIndex("name")
    }
    assert(exception.getMessage == "name is not contained in the schema")
  }

  it should "compare schemas for equality" in {
    val schema1 = schemaOf(idNameSpecs: _*)
    val schema2 = schemaOf(idNameSpecs: _*)
    val schema3 = schemaOf("id" -> AttributeType.INTEGER)

    assert(schema1 == schema2)
    assert(schema1 != schema3)
  }

  it should "return a proper string representation" in {
    val schema = schemaOf(idNameSpecs: _*)

    assert(
      schema.toString == "Schema[Attribute[name=id, type=integer], Attribute[name=name, type=string]]"
    )
  }

  it should "produce equal, deterministic hash codes for equal schemas" in {
    val a = schemaOf(idNameSpecs: _*)
    val b = schemaOf(idNameSpecs: _*)
    assert(a.hashCode == a.hashCode)
    assert(a == b && a.hashCode == b.hashCode)
    assert(Schema().hashCode == Schema().hashCode)
  }

  it should "not be equal to a non-Schema object" in {
    val schema = schemaOf("id" -> AttributeType.INTEGER)
    assert(!schema.equals("not a schema"))
    assert(!schema.equals(42))
    assert(!schema.equals(null))
  }

  it should "throw when add(Iterable) contains a name already in the schema" in {
    val schema = schemaOf("id" -> AttributeType.INTEGER)
    val ex = intercept[RuntimeException] {
      schema.add(attrs("id" -> AttributeType.STRING))
    }
    assert(ex.getMessage == "Cannot add attributes with duplicate names: id")
  }

  it should "throw when add(Attribute) name already exists" in {
    val schema = schemaOf("id" -> AttributeType.INTEGER)
    val ex = intercept[RuntimeException] {
      schema.add(new Attribute("id", AttributeType.LONG))
    }
    assert(ex.getMessage == "Attribute name 'id' already exists in the schema")
  }

  it should "round-trip through Jackson via the @JsonCreator constructor" in {
    val schema = schemaOf(idNameSpecs: _*)
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(schema), classOf[Schema])
    assert(restored == schema)
  }

  it should "reject duplicate adds and remove attributes case-insensitively while failing on absent removals" in {
    val schema = schemaOf("Id" -> AttributeType.INTEGER, "Name" -> AttributeType.STRING)
    val dupEx = intercept[RuntimeException] {
      schema.add(attrs("ID" -> AttributeType.STRING, "Extra" -> AttributeType.STRING))
    }
    assert(dupEx.getMessage == "Cannot add attributes with duplicate names: id")

    val removed = schema.remove(List("ID", "name"))
    assert(removed.getAttributes.isEmpty)

    val missingEx = intercept[IllegalArgumentException] {
      schema.remove(List("ID", "MissingCol"))
    }
    assert(missingEx.getMessage == "Cannot remove non-existent attributes: missingcol")
  }
}
