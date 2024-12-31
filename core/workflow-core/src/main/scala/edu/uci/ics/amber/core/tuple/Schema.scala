package edu.uci.ics.amber.core.tuple

import com.fasterxml.jackson.annotation.{JsonCreator, JsonIgnore, JsonProperty}
import com.google.common.base.Preconditions.checkNotNull

import scala.collection.immutable.ListMap

/**
  * Represents the schema of a tuple, consisting of a list of attributes.
  * The schema is immutable, and any modifications result in a new Schema instance.
  */
case class Schema @JsonCreator() (
    @JsonProperty(value = "attributes", required = true) attributes: List[Attribute] = List()
) extends Serializable {

  checkNotNull(attributes)

  // Maps attribute names (case-insensitive) to their indices in the schema.
  private val attributeIndex: Map[String, Int] =
    attributes.view.map(_.getName.toLowerCase).zipWithIndex.toMap

  def this(attrs: Attribute*) = this(attrs.toList)

  /**
    * Returns the list of attributes in the schema.
    */
  @JsonProperty(value = "attributes")
  def getAttributes: List[Attribute] = attributes

  /**
    * Returns a list of all attribute names in the schema.
    */
  @JsonIgnore
  def getAttributeNames: List[String] = attributes.map(_.getName)

  /**
    * Returns the index of a specified attribute by name.
    * Throws an exception if the attribute is not found.
    */
  def getIndex(attributeName: String): Int = {
    if (!containsAttribute(attributeName)) {
      throw new RuntimeException(s"$attributeName is not contained in the schema")
    }
    attributeIndex(attributeName.toLowerCase)
  }

  /**
    * Retrieves an attribute by its name.
    */
  def getAttribute(attributeName: String): Attribute = attributes(getIndex(attributeName))

  /**
    * Checks whether the schema contains an attribute with the specified name.
    */
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

  /**
    * Creates a new Schema containing only the specified attributes.
    */
  def getPartialSchema(attributeNames: List[String]): Schema = {
    Schema(attributeNames.map(name => getAttribute(name)))
  }

  /**
    * Converts the schema into a raw format where each attribute name
    * and attribute type are represented as strings. Useful for serialization across languages.
    */
  def toRawSchema: Map[String, String] =
    attributes.foldLeft(ListMap[String, String]())((list, attr) =>
      list + (attr.getName -> attr.getType.name())
    )

  /**
    * Creates a new Schema by adding multiple attributes to the current schema.
    */
  def add(attributesToAdd: Iterable[Attribute]): Schema = {
    val newAttributes = attributes ++ attributesToAdd
    Schema(newAttributes)
  }

  /**
    * Creates a new Schema by adding multiple attributes.
    * Accepts a variable number of `Attribute` arguments.
    */
  def add(attributes: Attribute*): Schema = {
    this.add(attributes)
  }

  /**
    * Creates a new Schema by adding a single attribute to the current schema.
    */
  def add(attribute: Attribute): Schema = add(List(attribute))

  /**
    * Creates a new Schema by adding an attribute with the specified name and type.
    */
  def add(attributeName: String, attributeType: AttributeType): Schema =
    add(List(new Attribute(attributeName, attributeType)))

  /**
    * Creates a new Schema by merging it with another schema.
    */
  def add(schema: Schema): Schema = {
    add(schema.attributes)
  }

  /**
    * Creates a new Schema by removing attributes with the specified names.
    */
  def remove(attributeNames: Iterable[String]): Schema = {
    val attributesToRemove = attributeNames.map(_.toLowerCase).toSet
    val remainingAttributes =
      attributes.filterNot(attr => attributesToRemove.contains(attr.getName.toLowerCase))
    Schema(remainingAttributes)
  }

  /**
    * Creates a new Schema by removing a single attribute with the specified name.
    */
  def remove(attributeName: String): Schema = remove(List(attributeName))
}

object Schema {

  /**
    * Creates a Schema instance from a raw map representation.
    * Each entry in the map contains an attribute name and its type as strings.
    */
  def fromRawSchema(raw: Map[String, String]): Schema = {
    Schema(raw.map {
      case (name, attrType) =>
        new Attribute(name, AttributeType.valueOf(attrType))
    }.toList)
  }
}
