package edu.uci.ics.amber.core.tuple

import org.scalatest.flatspec.AnyFlatSpec

class SchemaSpec extends AnyFlatSpec {

  "Schema" should "create an empty schema" in {
    val schema = Schema()
    assert(schema.getAttributes.isEmpty)
    assert(schema.getAttributeNames.isEmpty)
  }

  it should "create a schema with attributes of all types" in {
    val schema = Schema(
      List(
        new Attribute("stringAttr", AttributeType.STRING),
        new Attribute("integerAttr", AttributeType.INTEGER),
        new Attribute("longAttr", AttributeType.LONG),
        new Attribute("doubleAttr", AttributeType.DOUBLE),
        new Attribute("booleanAttr", AttributeType.BOOLEAN),
        new Attribute("timestampAttr", AttributeType.TIMESTAMP),
        new Attribute("binaryAttr", AttributeType.BINARY)
      )
    )
    assert(
      schema.getAttributes == List(
        new Attribute("stringAttr", AttributeType.STRING),
        new Attribute("integerAttr", AttributeType.INTEGER),
        new Attribute("longAttr", AttributeType.LONG),
        new Attribute("doubleAttr", AttributeType.DOUBLE),
        new Attribute("booleanAttr", AttributeType.BOOLEAN),
        new Attribute("timestampAttr", AttributeType.TIMESTAMP),
        new Attribute("binaryAttr", AttributeType.BINARY)
      )
    )
    assert(
      schema.getAttributeNames == List(
        "stringAttr",
        "integerAttr",
        "longAttr",
        "doubleAttr",
        "booleanAttr",
        "timestampAttr",
        "binaryAttr"
      )
    )
  }

  it should "add a single attribute using add(Attribute)" in {
    val schema = Schema()
    val updatedSchema = schema.add(new Attribute("id", AttributeType.INTEGER))

    assert(updatedSchema.getAttributes == List(new Attribute("id", AttributeType.INTEGER)))
  }

  it should "add multiple attributes using add(Attribute*)" in {
    val schema = Schema()
    val updatedSchema = schema.add(
      new Attribute("stringAttr", AttributeType.STRING),
      new Attribute("integerAttr", AttributeType.INTEGER),
      new Attribute("longAttr", AttributeType.LONG)
    )

    assert(
      updatedSchema.getAttributes == List(
        new Attribute("stringAttr", AttributeType.STRING),
        new Attribute("integerAttr", AttributeType.INTEGER),
        new Attribute("longAttr", AttributeType.LONG)
      )
    )
  }

  it should "add attributes from another schema using add(Schema)" in {
    val schema1 = Schema(List(new Attribute("id", AttributeType.INTEGER)))
    val schema2 = Schema(List(new Attribute("name", AttributeType.STRING)))

    val mergedSchema = schema1.add(schema2)

    assert(
      mergedSchema.getAttributes == List(
        new Attribute("id", AttributeType.INTEGER),
        new Attribute("name", AttributeType.STRING)
      )
    )
  }

  it should "add an attribute with name and type using add(String, AttributeType)" in {
    val schema = Schema()
    val updatedSchema = schema.add("id", AttributeType.INTEGER)

    assert(updatedSchema.getAttributes == List(new Attribute("id", AttributeType.INTEGER)))
  }

  it should "remove an existing attribute" in {
    val schema = Schema(
      List(
        new Attribute("id", AttributeType.INTEGER),
        new Attribute("name", AttributeType.STRING)
      )
    )

    val updatedSchema = schema.remove("id")

    assert(updatedSchema.getAttributes == List(new Attribute("name", AttributeType.STRING)))
  }

  it should "throw an exception when removing a non-existent attribute" in {
    val schema = Schema(
      List(new Attribute("id", AttributeType.INTEGER))
    )

    val exception = intercept[IllegalArgumentException] {
      schema.remove("name")
    }
    assert(exception.getMessage == "Cannot remove non-existent attributes: name")
  }

  it should "retrieve an attribute by name" in {
    val schema = Schema(
      List(
        new Attribute("id", AttributeType.INTEGER),
        new Attribute("name", AttributeType.STRING)
      )
    )

    val attribute = schema.getAttribute("id")

    assert(attribute == new Attribute("id", AttributeType.INTEGER))
  }

  it should "throw an exception when retrieving a non-existent attribute" in {
    val schema = Schema(List(new Attribute("id", AttributeType.INTEGER)))

    val exception = intercept[RuntimeException] {
      schema.getAttribute("name")
    }
    assert(exception.getMessage == "name is not contained in the schema")
  }

  it should "return a partial schema for attributes of all types" in {
    val schema = Schema(
      List(
        new Attribute("stringAttr", AttributeType.STRING),
        new Attribute("integerAttr", AttributeType.INTEGER),
        new Attribute("booleanAttr", AttributeType.BOOLEAN),
        new Attribute("doubleAttr", AttributeType.DOUBLE)
      )
    )

    val partialSchema = schema.getPartialSchema(List("stringAttr", "booleanAttr"))

    assert(
      partialSchema.getAttributes == List(
        new Attribute("stringAttr", AttributeType.STRING),
        new Attribute("booleanAttr", AttributeType.BOOLEAN)
      )
    )
  }

  it should "convert to raw schema and back for attributes of all types" in {
    val schema = Schema(
      List(
        new Attribute("stringAttr", AttributeType.STRING),
        new Attribute("integerAttr", AttributeType.INTEGER),
        new Attribute("longAttr", AttributeType.LONG),
        new Attribute("doubleAttr", AttributeType.DOUBLE),
        new Attribute("booleanAttr", AttributeType.BOOLEAN),
        new Attribute("timestampAttr", AttributeType.TIMESTAMP),
        new Attribute("binaryAttr", AttributeType.BINARY)
      )
    )

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
    val schema = Schema(
      List(
        new Attribute("stringAttr", AttributeType.STRING),
        new Attribute("integerAttr", AttributeType.INTEGER)
      )
    )

    assert(schema.containsAttribute("stringAttr"))
    assert(!schema.containsAttribute("nonExistentAttr"))
  }

  it should "return the index of an attribute by name" in {
    val schema = Schema(
      List(
        new Attribute("id", AttributeType.INTEGER),
        new Attribute("name", AttributeType.STRING)
      )
    )

    assert(schema.getIndex("id") == 0)
    assert(schema.getIndex("name") == 1)
  }

  it should "throw an exception when getting the index of a non-existent attribute" in {
    val schema = Schema(List(new Attribute("id", AttributeType.INTEGER)))

    val exception = intercept[RuntimeException] {
      schema.getIndex("name")
    }
    assert(exception.getMessage == "name is not contained in the schema")
  }

  it should "compare schemas for equality" in {
    val schema1 = Schema(
      List(
        new Attribute("id", AttributeType.INTEGER),
        new Attribute("name", AttributeType.STRING)
      )
    )
    val schema2 = Schema(
      List(
        new Attribute("id", AttributeType.INTEGER),
        new Attribute("name", AttributeType.STRING)
      )
    )
    val schema3 = Schema(
      List(
        new Attribute("id", AttributeType.INTEGER)
      )
    )

    assert(schema1 == schema2)
    assert(schema1 != schema3)
  }

  it should "return a proper string representation" in {
    val schema = Schema(
      List(
        new Attribute("id", AttributeType.INTEGER),
        new Attribute("name", AttributeType.STRING)
      )
    )

    assert(
      schema.toString == "Schema[Attribute[name=id, type=integer], Attribute[name=name, type=string]]"
    )
  }
}
