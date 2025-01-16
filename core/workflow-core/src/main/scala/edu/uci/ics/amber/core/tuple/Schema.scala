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

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if (attributes == null) 0 else attributes.hashCode)
    result = prime * result + (if (attributeIndex == null) 0 else attributeIndex.hashCode)
    result
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: Schema => this.attributes == that.attributes
      case _            => false
    }
  }

  override def toString: String = s"Schema[${attributes.map(_.toString).mkString(", ")}]"

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
    * Throws an exception if any attribute name already exists in the schema.
    */
  def add(attributesToAdd: Iterable[Attribute]): Schema = {
    val existingNames = this.getAttributeNames.map(_.toLowerCase).toSet
    val duplicateNames = attributesToAdd.map(_.getName.toLowerCase).toSet.intersect(existingNames)

    if (duplicateNames.nonEmpty) {
      throw new RuntimeException(
        s"Cannot add attributes with duplicate names: ${duplicateNames.mkString(", ")}"
      )
    }

    val newAttributes = attributes ++ attributesToAdd
    Schema(newAttributes)
  }

  /**
    * Creates a new Schema by adding multiple attributes.
    * Accepts a variable number of `Attribute` arguments.
    * Throws an exception if any attribute name already exists in the schema.
    */
  def add(attributes: Attribute*): Schema = {
    this.add(attributes)
  }

  /**
    * Creates a new Schema by adding a single attribute to the current schema.
    * Throws an exception if the attribute name already exists in the schema.
    */
  def add(attribute: Attribute): Schema = {
    if (containsAttribute(attribute.getName)) {
      throw new RuntimeException(
        s"Attribute name '${attribute.getName}' already exists in the schema"
      )
    }
    add(List(attribute))
  }

  /**
    * Creates a new Schema by adding an attribute with the specified name and type.
    * Throws an exception if the attribute name already exists in the schema.
    */
  def add(attributeName: String, attributeType: AttributeType): Schema =
    add(new Attribute(attributeName, attributeType))

  /**
    * Creates a new Schema by merging it with another schema.
    * Throws an exception if there are duplicate attribute names.
    */
  def add(schema: Schema): Schema = {
    add(schema.attributes)
  }

  /**
    * Creates a new Schema by removing attributes with the specified names.
    * Throws an exception if any of the specified attributes do not exist in the schema.
    */
  def remove(attributeNames: Iterable[String]): Schema = {
    val attributesToRemove = attributeNames.map(_.toLowerCase).toSet

    // Check for non-existent attributes
    val nonExistentAttributes = attributesToRemove.diff(attributes.map(_.getName.toLowerCase).toSet)
    if (nonExistentAttributes.nonEmpty) {
      throw new IllegalArgumentException(
        s"Cannot remove non-existent attributes: ${nonExistentAttributes.mkString(", ")}"
      )
    }

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
