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

import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnore, JsonProperty}
import com.google.common.base.Preconditions.checkNotNull
import org.apache.texera.amber.core.tuple.Tuple.checkSchemaMatchesFields
import org.ehcache.sizeof.SizeOf

import java.util
import scala.collection.mutable

class TupleBuildingException(errorMessage: String) extends RuntimeException(errorMessage) {}

/**
  * Represents a tuple in a data processing workflow, encapsulating a schema and corresponding field values.
  *
  * A Tuple is a fundamental data structure that holds an ordered collection of elements. Each element can be of any type.
  * The schema defines the structure of the Tuple, including the names and types of fields that the Tuple can hold.
  *
  * @constructor Create a new Tuple with a specified schema and field values.
  * @param schema The schema associated with this tuple, defining the structure and types of fields in the tuple.
  * @param fieldVals A list of values corresponding to the fields defined in the schema. Each value in this list
  *                  is mapped to a field in the schema, in the same order as the fields are defined.
  *
  * @throws java.lang.IllegalArgumentException if either schema or fieldVals is null, ensuring that every Tuple has a well-defined structure.
  */
case class Tuple @JsonCreator() (
    @JsonProperty(value = "schema", required = true) schema: Schema,
    @JsonProperty(value = "fields", required = true) fieldVals: Array[Any]
) extends SeqTupleLike
    with Serializable {

  checkNotNull(schema)
  checkNotNull(fieldVals)
  checkSchemaMatchesFields(schema.getAttributes, fieldVals)

  // Lazy so deepSizeOf runs only when the size is read (row-path stats); the columnar path never
  // reads it, so it pays nothing. JsonIgnore keeps this derived field out of the serialized form.
  @JsonIgnore override lazy val inMemSize: Long = SizeOf.newInstance().deepSizeOf(this)

  @JsonIgnore def length: Int = fieldVals.length

  @JsonIgnore def getSchema: Schema = schema

  def getField[T](index: Int): T = {
    fieldVals(index).asInstanceOf[T]
  }

  def getField[T](attributeName: String): T = {
    if (!schema.containsAttribute(attributeName)) {
      throw new RuntimeException(s"$attributeName is not in the tuple")
    }
    getField(schema.getIndex(attributeName))
  }

  def getField[T](attribute: Attribute): T = getField(attribute.getName)

  override def getFields: Array[Any] = fieldVals

  override def enforceSchema(schema: Schema): Tuple = {
    assert(
      getSchema == schema,
      s"output tuple schema does not match the expected schema! " +
        s"output schema: $getSchema, " +
        s"expected schema: $schema"
    )
    this
  }

  override def hashCode: Int = util.Arrays.deepHashCode(getFields.asInstanceOf[Array[AnyRef]])

  override def equals(obj: Any): Boolean =
    obj match {
      case that: Tuple if this.schema == that.schema =>
        val left = this.getFields
        val right = that.getFields
        val fieldCount = math.min(left.length, right.length)
        var index = 0
        while (index < fieldCount && fieldEquals(left(index), right(index))) index += 1
        index == fieldCount
      case _ => false
    }

  private def fieldEquals(left: Any, right: Any): Boolean =
    if (left.isInstanceOf[Array[Byte]] && right.isInstanceOf[Array[Byte]])
      util.Arrays.equals(left.asInstanceOf[Array[Byte]], right.asInstanceOf[Array[Byte]])
    else left == right

  def getPartialTuple(attributeNames: List[String]): Tuple = {
    val partialSchema = schema.getPartialSchema(attributeNames)
    val builder = Tuple.Builder(partialSchema)
    val partialArray = new Array[Any](attributeNames.length)
    val names = attributeNames.iterator
    var index = 0
    while (names.hasNext) {
      partialArray(index) = getField[Any](names.next())
      index += 1
    }
    builder.addSequentially(partialArray)
    builder.build()
  }

  override def toString: String =
    s"Tuple [schema=$schema, fields=${fieldVals.mkString("[", ", ", "]")}]"
}

object Tuple {

  /**
    * Validates that the provided attributes match the provided fields in type and order.
    *
    * @param attributes An iterable of Attributes to be validated against the fields.
    * @param fields     An iterable of field values to be validated against the attributes.
    * @throws RuntimeException if the sizes of attributes and fields do not match, or if their types are incompatible.
    */
  private def checkSchemaMatchesFields(
      attributes: Iterable[Attribute],
      fields: Iterable[Any]
  ): Unit = {
    val attributeList = attributes.toList
    val fieldList = fields.toList

    if (attributeList.size != fieldList.size) {
      throw new RuntimeException(
        s"Schema size (${attributeList.size}) and field size (${fieldList.size}) are different"
      )
    }

    val fieldIterator = fieldList.iterator
    attributeList.foreach(attribute => checkAttributeMatchesField(attribute, fieldIterator.next()))
  }

  /**
    * Validates that a single field matches its corresponding attribute in type.
    *
    * @param attribute The attribute to be matched.
    * @param field     The field value to be checked.
    * @throws RuntimeException if the field's type does not match the attribute's defined type.
    */
  private def checkAttributeMatchesField(attribute: Attribute, field: Any): Unit = {
    if (
      field != null && attribute.getType != AttributeType.ANY && !field.getClass.equals(
        attribute.getType.getFieldClass
      )
    ) {
      throw new RuntimeException(
        s"Attribute ${attribute.getName}'s type (${attribute.getType}) is different from field's type (${AttributeType
          .getAttributeType(field.getClass)})"
      )
    }
  }

  /**
    * Creates a new Tuple builder for a specified schema.
    *
    * @param schema The schema for which the Tuple builder will create Tuples.
    * @return A new instance of Tuple.Builder configured with the specified schema.
    */
  def builder(schema: Schema): Builder = {
    Tuple.Builder(schema)
  }

  /**
    * Builder class for constructing Tuple instances in a flexible and controlled manner.
    */
  case class Builder(schema: Schema) {
    private val fieldNameMap = mutable.Map.empty[String, Any]

    def add(tuple: Tuple, isStrictSchemaMatch: Boolean = true): Builder = {
      require(tuple != null, "Tuple cannot be null")

      val attributes = tuple.schema.getAttributes.iterator
      tuple.getFields.foreach { field =>
        val attribute = attributes.next()
        if (isStrictSchemaMatch || schema.containsAttribute(attribute.getName)) {
          this.add(attribute, field)
        }
      }
      this
    }

    def add(attribute: Attribute, field: Any): Builder = {
      require(attribute != null, "Attribute cannot be null")
      checkAttributeMatchesField(attribute, field)

      if (!schema.containsAttribute(attribute.getName)) {
        throw new TupleBuildingException(
          s"${attribute.getName} doesn't exist in the expected schema."
        )
      }

      fieldNameMap.put(attribute.getName.toLowerCase, field)
      this
    }

    def add(attributeName: String, attributeType: AttributeType, field: Any): Builder = {
      require(
        attributeName != null && attributeType != null,
        "Attribute name and type cannot be null"
      )
      this.add(new Attribute(attributeName, attributeType), field)
      this
    }

    def addSequentially(fields: Array[Any]): Builder = {
      require(fields != null, "Fields cannot be null")
      checkSchemaMatchesFields(schema.getAttributes, fields)
      val attributes = schema.getAttributes.iterator
      fields.foreach(field => this.add(attributes.next(), field))
      this
    }

    def build(): Tuple = {
      val missingAttributes =
        schema.getAttributes.filterNot(attr => fieldNameMap.contains(attr.getName.toLowerCase))
      if (missingAttributes.nonEmpty) {
        throw new TupleBuildingException(
          s"Tuple does not have the same number of attributes as schema. Missing attributes are $missingAttributes"
        )
      }

      val attributes = schema.getAttributes
      val fields = new Array[Any](attributes.length)
      val attributeIterator = attributes.iterator
      var index = 0
      while (attributeIterator.hasNext) {
        fields(index) = fieldNameMap(attributeIterator.next().getName.toLowerCase)
        index += 1
      }
      new Tuple(schema, fields)
    }
  }
}
