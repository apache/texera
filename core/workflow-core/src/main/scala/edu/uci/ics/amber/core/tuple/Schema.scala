package edu.uci.ics.amber.core.tuple

import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnore, JsonProperty}
import com.google.common.base.Preconditions.checkNotNull

import scala.collection.immutable.ListMap

case class Schema @JsonCreator() (
                                   @JsonProperty(value = "attributes", required = true) attributes: List[Attribute] = List()
                                 ) extends Serializable {

  checkNotNull(attributes)

  private val attributeIndex: Map[String, Int] =
    attributes.view.map(_.getName.toLowerCase).zipWithIndex.toMap

  def this(attrs: Attribute*) = this(attrs.toList)

  @JsonProperty(value = "attributes")
  def getAttributes: List[Attribute] = attributes

  @JsonIgnore
  def getAttributeNames: List[String] = attributes.map(_.getName)

  def getIndex(attributeName: String): Int = {
    if (!containsAttribute(attributeName)) {
      throw new RuntimeException(s"$attributeName is not contained in the schema")
    }
    attributeIndex(attributeName.toLowerCase)
  }

  def getAttribute(attributeName: String): Attribute = attributes(getIndex(attributeName))

  @JsonIgnore
  def containsAttribute(attributeName: String): Boolean =
    attributeIndex.contains(attributeName.toLowerCase)

  override def hashCode(): Int = attributes.hashCode

  override def equals(obj: Any): Boolean =
    obj match {
      case that: Schema => this.attributes == that.attributes
      case _            => false
    }

  override def toString: String = s"Schema[$attributes]"

  def getPartialSchema(attributeNames: List[String]): Schema = {
    Schema(attributeNames.map(name => getAttribute(name)))
  }

  /**
    * This method converts the schema into a raw format where each attribute name
    * and attribute type are represented as strings. This is for serialization between languages.
    */
  def toRawSchema: Map[String, String] =
    attributes.foldLeft(ListMap[String, String]())((list, attr) =>
      list + (attr.getName -> attr.getType.name())
    )

  /**
    * Creates a new Schema by adding attributes to the current Schema.
    */
  def add(attributesToAdd: Iterable[Attribute]): Schema = {
    val newAttributes = attributes ++ attributesToAdd
    Schema(newAttributes)
  }

  /**
    * Creates a new Schema by removing attributes from the current Schema.
    */
  def remove(attributeNames: Iterable[String]): Schema = {
    val attributesToRemove = attributeNames.map(_.toLowerCase).toSet
    val remainingAttributes = attributes.filterNot(attr =>
      attributesToRemove.contains(attr.getName.toLowerCase)
    )
    Schema(remainingAttributes)
  }

  /**
    * Creates a new Schema by adding a single attribute.
    */
  def add(attribute: Attribute): Schema = add(List(attribute))

  /**
    * Creates a new Schema by adding a single attribute.
    */
  def add(attributeName: String, attributeType: AttributeType): Schema = add(List(new Attribute(attributeName, attributeType)))

  /**
    * Creates a new Schema by adding a Schema.
    */
  def add(schema: Schema): Schema = {
    add(schema.attributes)
  }

  /**
    * Creates a new Schema by removing a single attribute.
    */
  def remove(attributeName: String): Schema = remove(List(attributeName))
}

object Schema {

  /**
    * Creates a Schema instance from a raw schema map.
    */
  def fromRawSchema(raw: Map[String, String]): Schema = {
    Schema(raw.map { case (name, attrType) =>
      new Attribute(name, AttributeType.valueOf(attrType))
    }.toList)
  }
}