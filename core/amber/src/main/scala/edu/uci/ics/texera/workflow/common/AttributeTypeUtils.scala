package edu.uci.ics.texera.workflow.common

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

import scala.util.control.Exception.allCatch

object AttributeTypeUtils {

  /**
    * this loop check whether the current attribute in the array is the attribute for casting,
    * if it is, change it to result type
    * if it's not, remain the same type
    * we need this loop to keep the order the same as the original
    * @param schema schema of data
    * @param attribute selected attribute
    * @param resultType casting type
    * @return schema of data
    */
  def SchemaCasting(
      schema: Schema,
      attribute: String,
      resultType: AttributeType
  ): Schema = {
    // need a builder to maintain the order of original schema
    val builder = Schema.newBuilder
    val attributes: List[Attribute] = schema.getAttributesScala
    // change the schema when meet selected attribute else remain the same
    for (i <- attributes.indices) {
      if (attributes.apply(i).getName.equals(attribute)) {
        (resultType) match {
          case AttributeType.STRING    => builder.add(attribute, resultType)
          case AttributeType.INTEGER   => builder.add(attribute, resultType)
          case AttributeType.DOUBLE    => builder.add(attribute, resultType)
          case AttributeType.LONG      => builder.add(attribute, resultType)
          case AttributeType.BOOLEAN   => builder.add(attribute, resultType)
          case AttributeType.TIMESTAMP => builder.add(attribute, AttributeType.STRING)
          case AttributeType.ANY       => builder.add(attribute, AttributeType.STRING)
          case _                       => builder.add(attribute, AttributeType.STRING)
        }
      } else {
        builder.add(attributes.apply(i).getName, attributes.apply(i).getType)
      }
    }
    builder.build()
  }

  /**
    * Casting the tuple and return a new tuple with casted type
    * @param t tuple to be processed
    * @param attribute selected attribute
    * @param resultType casting type
    * @return casted tuple
    */
  def TupleCasting(
      t: Tuple,
      attribute: String,
      resultType: AttributeType
  ): Tuple = {
    // need a builder to maintain the order of original tuple
    val builder: Tuple.Builder = Tuple.newBuilder
    val attributes: List[Attribute] = t.getSchema.getAttributesScala
    // change the tuple when meet selected attribute else remain the same
    for (i <- attributes.indices) {
      if (attributes.apply(i).getName.equals(attribute)) {
        val field: String = t.get(i).toString
        (resultType) match {
          case AttributeType.STRING    => builder.add(attribute, resultType, field)
          case AttributeType.INTEGER   => builder.add(attribute, resultType, field.toInt)
          case AttributeType.DOUBLE    => builder.add(attribute, resultType, field.toDouble)
          case AttributeType.LONG      => builder.add(attribute, resultType, field.toLong)
          case AttributeType.BOOLEAN   => builder.add(attribute, resultType, field.toBoolean)
          case AttributeType.TIMESTAMP => builder.add(attribute, AttributeType.STRING, field)
          case AttributeType.ANY       => builder.add(attribute, AttributeType.STRING, field)
          case _                       => builder.add(attribute, resultType, field)
        }
      } else {
        builder.add(attributes.apply(i).getName, attributes.apply(i).getType, t.get(i))
      }
    }
    builder.build()
  }

  /**
    * parse Field to corresponding Java object Type base on given Schema AttributeType
    * @param attributeTypeList Schema AttributeTypeList
    * @param fields fields value, originally is String
    * @return parsedFields
    */
  def parseField(
      attributeTypeList: Array[AttributeType],
      fields: Array[String]
  ): Array[Object] = {
    val parsedFields: Array[Object] = new Array[Object](fields.length)
    for (i <- fields.indices) {
      attributeTypeList.apply(i) match {
        case AttributeType.INTEGER => parsedFields.update(i, Integer.valueOf(fields.apply(i)))
        case AttributeType.LONG    => parsedFields.update(i, java.lang.Long.valueOf(fields.apply(i)))
        case AttributeType.DOUBLE =>
          parsedFields.update(i, java.lang.Double.valueOf(fields.apply(i)))
        case AttributeType.BOOLEAN =>
          parsedFields.update(i, java.lang.Boolean.valueOf(fields.apply(i)))
        case AttributeType.STRING    => parsedFields.update(i, fields.apply(i))
        case AttributeType.TIMESTAMP =>
        case AttributeType.ANY       =>
        case _                       => parsedFields.update(i, fields.apply(i))
      }
    }
    parsedFields
  }

  /**
    * Infers field types of a given row of data. The given attributeTypeList will be updated
    * through each iteration of row inference, to contain the must accurate inference.
    * @param attributeTypeList AttributeTypes that being passed to each iteration.
    * @param fields data fields to be parsed, originally as String fields
    * @return
    */
  def inferRow(
      attributeTypeList: Array[AttributeType],
      fields: Array[String]
  ): Unit = {
    for (i <- fields.indices) {
      attributeTypeList.update(i, inferField(attributeTypeList.apply(i), fields.apply(i)))
    }
  }

  /**
    * Infers field types of a given row of data.
    * @param fields data fields to be parsed, originally as String fields
    * @return AttributeType array
    */
  def inferRow(fields: Array[String]): Array[AttributeType] = {
    val attributeTypeList: Array[AttributeType] =
      Array.fill[AttributeType](fields.length)(AttributeType.INTEGER)
    for (i <- fields.indices) {
      attributeTypeList.update(i, inferField(fields.apply(i)))
    }
    attributeTypeList
  }

  /**
    * infer filed type with only data field
    * @param fieldValue data field to be parsed, original as String field
    * @return inferred AttributeType
    */
  def inferField(fieldValue: String): AttributeType = {
    tryParseInteger(fieldValue)
  }

  /**
    * InferField when get both typeSofar and tuple string
    * @param attributeType typeSofar
    * @param fieldValue data field to be parsed, original as String field
    * @return inferred AttributeType
    */
  def inferField(attributeType: AttributeType, fieldValue: String): AttributeType = {
    attributeType match {
      case AttributeType.STRING  => tryParseString()
      case AttributeType.BOOLEAN => tryParseBoolean(fieldValue)
      case AttributeType.DOUBLE  => tryParseDouble(fieldValue)
      case AttributeType.LONG    => tryParseLong(fieldValue)
      case AttributeType.INTEGER => tryParseInteger(fieldValue)
      case _                     => tryParseString()
    }
  }

  private def tryParseInteger(fieldValue: String): AttributeType = {
    allCatch opt fieldValue.toInt match {
      case Some(_) => AttributeType.INTEGER
      case None    => tryParseLong(fieldValue)
    }
  }

  private def tryParseLong(fieldValue: String): AttributeType = {
    allCatch opt fieldValue.toLong match {
      case Some(_) => AttributeType.LONG
      case None    => tryParseDouble(fieldValue)
    }
  }

  private def tryParseDouble(fieldValue: String): AttributeType = {
    allCatch opt fieldValue.toDouble match {
      case Some(_) => AttributeType.DOUBLE
      case None    => tryParseBoolean(fieldValue)
    }
  }
  private def tryParseBoolean(fieldValue: String): AttributeType = {
    allCatch opt fieldValue.toBoolean match {
      case Some(_) => AttributeType.BOOLEAN
      case None    => tryParseString()
    }
  }
  private def tryParseString(): AttributeType = {
    AttributeType.STRING
  }

}
