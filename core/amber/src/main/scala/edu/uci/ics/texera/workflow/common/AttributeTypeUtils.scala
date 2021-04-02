package edu.uci.ics.texera.workflow.common

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType._

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.format.DateTimeParseException
import scala.util.control.Exception.allCatch

object AttributeTypeUtils extends Serializable {

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
        resultType match {
          case STRING | INTEGER | DOUBLE | LONG | BOOLEAN | TIMESTAMP =>
            builder.add(attribute, resultType)
          case ANY | _ =>
            builder.add(attribute, attributes.apply(i).getType)
        }
      } else {
        builder.add(attributes.apply(i).getName, attributes.apply(i).getType)
      }
    }
    builder.build()
  }

  /**
    * Casting the tuple and return a new tuple with casted type
    * @param tuple tuple to be processed
    * @param attribute selected attribute
    * @param resultType casting type
    * @return casted tuple
    */
  def TupleCasting(
      tuple: Tuple,
      attribute: String,
      resultType: AttributeType
  ): Tuple = {
    // need a builder to maintain the order of original tuple
    val builder: Tuple.Builder = Tuple.newBuilder
    val attributes: List[Attribute] = tuple.getSchema.getAttributesScala
    // change the tuple when meet selected attribute else remain the same
    for (i <- attributes.indices) {
      if (attributes.apply(i).getName.equals(attribute)) {
        val field: Object = tuple.get(i)
        resultType match {
          case STRING | INTEGER | DOUBLE | LONG | BOOLEAN | TIMESTAMP =>
            builder.add(attribute, resultType, parseField(field, resultType))
          case ANY | _ => builder.add(attribute, attributes.apply(i).getType, tuple.get(i))
        }
      } else {
        builder.add(attributes.apply(i).getName, attributes.apply(i).getType, tuple.get(i))
      }
    }
    builder.build()
  }

  /**
    * parse Fields to corresponding Java objects base on the given Schema AttributeTypes
    * @param attributeTypes Schema AttributeTypeList
    * @param fields fields value
    * @return parsedFields in the target AttributeTypes
    */
  @throws[AttributeTypeException]
  def parseFields(
      fields: Array[Object],
      attributeTypes: Array[AttributeType]
  ): Array[Object] = {
    fields.indices.map(i => parseField(fields.apply(i), attributeTypes.apply(i))).toArray
  }

  /**
    * parse Field to a corresponding Java object base on the given Schema AttributeType
    * @param field fields value
    * @param attributeType target AttributeType
    *
    * @return parsedField in the target AttributeType
    */
  @throws[AttributeTypeException]
  def parseField(
      field: Object,
      attributeType: AttributeType
  ): Object = {

    attributeType match {
      case INTEGER   => parseInteger(field)
      case LONG      => parseLong(field)
      case DOUBLE    => parseDouble(field)
      case BOOLEAN   => parseBoolean(field)
      case TIMESTAMP => parseTimestamp(field)
      case STRING    => field.toString
      case ANY | _   => field
    }
  }

  /**
    * Infers field types of a given row of data. The given attributeTypes will be updated
    * through each iteration of row inference, to contain the most accurate inference.
    * @param attributeTypes AttributeTypes that being passed to each iteration.
    * @param fields data fields to be parsed
    * @return
    */
  def inferRow(
      attributeTypes: Array[AttributeType],
      fields: Array[Object]
  ): Unit = {
    for (i <- fields.indices) {
      attributeTypes.update(i, inferField(attributeTypes.apply(i), fields.apply(i)))
    }
  }

  /**
    * Infers field types of a given row of data.
    * @param fieldsIterator iterator of field arrays to be parsed.
    *                       each field array should have exact same order and length.
    * @return AttributeType array
    */
  def inferSchemaFromRows(fieldsIterator: Iterator[Array[Object]]): Array[AttributeType] = {
    var attributeTypes: Array[AttributeType] = Array()

    for (fields <- fieldsIterator) {
      if (attributeTypes.isEmpty) {
        attributeTypes = Array.fill[AttributeType](fields.length)(INTEGER)
      }
      inferRow(attributeTypes, fields)
    }
    attributeTypes
  }

  /**
    * infer filed type with only data field
    * @param fieldValue data field to be parsed, original as String field
    * @return inferred AttributeType
    */
  def inferField(fieldValue: Object): AttributeType = {
    tryParseInteger(fieldValue)
  }

  /**
    * InferField when get both typeSofar and tuple string
    * @param attributeType typeSofar
    * @param fieldValue data field to be parsed, original as String field
    * @return inferred AttributeType
    */
  def inferField(attributeType: AttributeType, fieldValue: Object): AttributeType = {
    attributeType match {
      case STRING  => tryParseString()
      case BOOLEAN => tryParseBoolean(fieldValue)
      case DOUBLE  => tryParseDouble(fieldValue)
      case LONG    => tryParseLong(fieldValue)
      case INTEGER => tryParseInteger(fieldValue)
      case _       => tryParseString()
    }
  }

  @throws[AttributeTypeException]
  def parseInteger(fieldValue: Object): Integer = {
    fieldValue match {
      case str: String                => str.toInt
      case int: Integer               => int
      case long: java.lang.Long       => long.toInt
      case double: java.lang.Double   => double.toInt
      case boolean: java.lang.Boolean => if (boolean) 1 else 0
      // Timestamp is considered to be illegal here.
      case _ =>
        throw new AttributeTypeException(
          s"not able to parse type ${fieldValue.getClass} to Integer."
        )
    }
  }

  @throws[AttributeTypeException]
  def parseBoolean(fieldValue: Object): java.lang.Boolean = {
    fieldValue match {
      case str: String =>
        if (str.trim.equalsIgnoreCase("true"))
          true
        else if (str.trim.equalsIgnoreCase("false"))
          false
        else
          str.trim.toBoolean
      case int: Integer               => int != 0
      case long: java.lang.Long       => long != 0
      case double: java.lang.Double   => double != 0
      case boolean: java.lang.Boolean => boolean
      // Timestamp is considered to be illegal here.
      case _ =>
        throw new AttributeTypeException(
          s"not able to parse type ${fieldValue.getClass} to Boolean."
        )
    }
  }

  @throws[AttributeTypeException]
  def parseLong(fieldValue: Object): java.lang.Long = {
    fieldValue match {
      case str: String =>
        try str.toLong
        catch {
          case _: NumberFormatException => parseTimestamp(fieldValue).getTime
        }
      case int: Integer               => int.toLong
      case long: java.lang.Long       => long
      case double: java.lang.Double   => double.toLong
      case boolean: java.lang.Boolean => if (boolean) 1L else 0L
      case timestamp: Timestamp       => timestamp.toInstant.toEpochMilli
      case _ =>
        throw new AttributeTypeException(s"not able to parse type ${fieldValue.getClass} to Long.")
    }
  }

  @throws[AttributeTypeException]
  def parseDouble(fieldValue: Object): java.lang.Double = {
    fieldValue match {
      case str: String                => str.toDouble
      case int: Integer               => int.toDouble
      case long: java.lang.Long       => long.toDouble
      case double: java.lang.Double   => double
      case boolean: java.lang.Boolean => if (boolean) 1 else 0
      // Timestamp is considered to be illegal here.
      case _ =>
        throw new AttributeTypeException(
          s"not able to parse type ${fieldValue.getClass} to Double."
        )
    }
  }

  @throws[AttributeTypeException]
  def parseTimestamp(fieldValue: Object): Timestamp = {
    fieldValue match {
      case str: String =>
        try new Timestamp(Instant.parse(str).toEpochMilli)
        catch {
          case _: DateTimeParseException =>
            try Timestamp.valueOf(str)
            catch {
              case _: IllegalArgumentException =>
                val utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                new Timestamp(utcFormat.parse(fieldValue.toString).getTime)

            }
        }
      case long: java.lang.Long => new Timestamp(long)
      // Integer, Long, Double and Boolean are considered to be illegal here.
      case _ =>
        throw new AttributeTypeException(
          s"not able to parse type ${fieldValue.getClass} to Timestamp."
        )
    }
  }

  private def tryParseInteger(fieldValue: Object): AttributeType = {
    allCatch opt parseInteger(fieldValue) match {
      case Some(_) => INTEGER
      case None    => tryParseLong(fieldValue)
    }
  }

  private def tryParseLong(fieldValue: Object): AttributeType = {
    allCatch opt parseLong(fieldValue) match {
      case Some(_) => LONG
      case None    => tryParseDouble(fieldValue)
    }
  }

  private def tryParseDouble(fieldValue: Object): AttributeType = {
    allCatch opt parseDouble(fieldValue) match {
      case Some(_) => DOUBLE
      case None    => tryParseBoolean(fieldValue)
    }
  }

  private def tryParseBoolean(fieldValue: Object): AttributeType = {
    allCatch opt parseBoolean(fieldValue) match {
      case Some(_) => BOOLEAN
      case None    => tryParseString()
    }
  }

  private def tryParseString(): AttributeType = {
    STRING
  }

  class AttributeTypeException(msg: String) extends IllegalArgumentException(msg) {}
}
