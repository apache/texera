package edu.uci.ics.texera.workflow.common.tuple

import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnore, JsonProperty}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.JsonNode
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.engine.common.tuple.amber.SeqTupleLike
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.workflow.common.tuple.Tuple.checkSchemaMatchesFields
import edu.uci.ics.texera.workflow.common.tuple.exception.TupleBuildingException
import org.bson.Document
import org.ehcache.sizeof.SizeOf

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

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
  * @throws IllegalArgumentException if either schema or fieldVals is null, ensuring that every Tuple has a well-defined structure.
  */
case class Tuple @JsonCreator() (
    @JsonProperty(value = "schema", required = true) schema: Schema,
    @JsonProperty(value = "fields", required = true) fieldVals: List[Any]
) extends SeqTupleLike
    with Serializable {

  require(schema != null, "Schema cannot be null")
  require(fieldVals != null, "Fields cannot be null")
  checkSchemaMatchesFields(schema.getAttributes.asScala, fieldVals)

  override val inMemSize: Long = SizeOf.newInstance().deepSizeOf(this)

  @JsonIgnore def length: Int = fieldVals.size

  @JsonIgnore def get(i: Int): Any = fieldVals(i)

  @JsonIgnore def getSchema: Schema = schema

  def getField[T](attributeName: String): T = {
    if (!schema.containsAttribute(attributeName)) {
      throw new RuntimeException(s"$attributeName is not in the tuple")
    }
    fieldVals(schema.getIndex(attributeName)).asInstanceOf[T]
  }

  def getField[T](attributeName: String, fieldClass: Class[T]): T = getField[T](attributeName)

  def getFields: Seq[Any] = fieldVals

  override def hashCode: Int = util.Arrays.deepHashCode(fields.map(_.asInstanceOf[AnyRef]))

  override def equals(obj: Any): Boolean =
    obj match {
      case that: Tuple => (this.fields sameElements that.fields) && this.schema == that.schema
      case _           => false
    }

  def getPartialTuple(indices: Array[Int]): Tuple = {
    val partialSchema = schema.getPartialSchema(indices)
    val builder = Tuple.Builder(partialSchema)
    val partialArray = indices.map(fieldVals(_))
    builder.addSequentially(partialArray)
    builder.build()
  }

  override def toString: String = s"Tuple [schema=$schema, fields=$fieldVals]"

  def asKeyValuePairJson(): ObjectNode = {
    val objectNode = Utils.objectMapper.createObjectNode()
    this.schema.getAttributeNames.asScala.foreach { attrName =>
      val valueNode = Utils.objectMapper.convertValue(this.getField(attrName), classOf[JsonNode])
      objectNode.set[ObjectNode](attrName, valueNode)
    }
    objectNode
  }

  def asDocument(): Document = {
    val doc = new Document()
    this.schema.getAttributeNames.asScala.foreach { attrName =>
      doc.put(attrName, this.getField(attrName))
    }
    doc
  }

  override def fields: Array[Any] = fieldVals.toArray
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

    (attributeList zip fieldList).foreach {
      case (attribute, field) =>
        checkAttributeMatchesField(attribute, field)
    }
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

      tuple.fields.zipWithIndex.foreach {
        case (field, i) =>
          val attribute = tuple.schema.getAttributes.get(i)
          if (!isStrictSchemaMatch && !schema.containsAttribute(attribute.getName)) {
            // Skip if not matching in non-strict mode
          } else {
            add(attribute, tuple.fields(i))
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
      checkSchemaMatchesFields(schema.getAttributes.asScala, fields)
      schema.getAttributes.asScala.zip(fields).foreach {
        case (attribute, field) =>
          this.add(attribute, field)
      }
      this
    }

    def build(): Tuple = {
      val missingAttributes = schema.getAttributes.asScala.filterNot(attr =>
        fieldNameMap.contains(attr.getName.toLowerCase)
      )
      if (missingAttributes.nonEmpty) {
        throw new TupleBuildingException(
          s"Tuple does not have the same number of attributes as schema. Missing attributes are $missingAttributes"
        )
      }

      val fields =
        schema.getAttributes.asScala.map(attr => fieldNameMap(attr.getName.toLowerCase)).toList
      new Tuple(schema, fields)
    }
  }
}
