package edu.uci.ics.texera.workflow.common.tuple.schema
import com.github.sisyphsu.dateparser.DateParserUtils
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeType._
import edu.uci.ics.texera.workflow.operators.typecasting.TypeCastingUnit

import java.sql.Timestamp
import scala.util.Try
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
    val builder = Schema.builder()
    val attributes: List[Attribute] = schema.getAttributes
    // change the schema when meet selected attribute else remain the same
    for (i <- attributes.indices) {
      if (attributes.apply(i).getName.equals(attribute)) {
        resultType match {
          case STRING | INTEGER | DOUBLE | LONG | BOOLEAN | TIMESTAMP | BINARY =>
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
    * Casts the fields of a tuple to new types according to a list of type casting units,
    * producing a new tuple that conforms to the specified type changes.
    * Each type casting unit specifies the attribute name and the target type to cast to.
    * If an attribute name in the tuple does not have a corresponding type casting unit,
    * its value is included in the result tuple without type conversion.
    *
    * @param tuple           The source tuple whose fields are to be casted.
    * @param typeCastingUnits A list of type casting units specifying the attribute names
    *                         and their corresponding target types for casting.
    * @return                A new instance of TupleLike with fields casted to the target types
    *                        as specified by the typeCastingUnits.
    */
  def tupleCasting(
      tuple: Tuple,
      typeCastingUnits: List[TypeCastingUnit]
  ): TupleLike =
    TupleLike(
      tuple.getSchema.getAttributes.map { attr =>
        val targetType = typeCastingUnits
          .find(_.attribute == attr.getName)
          .map(_.resultType)
          .getOrElse(attr.getType)

        parseField(tuple.getField(attr.getName), targetType)
      }
    )

  def parseFields(fields: Array[Any], schema: Schema): Array[Any] = {
    parseFields(fields, schema.getAttributes.map(attr => attr.getType).toArray)
  }

  /**
    * parse Fields to corresponding Java objects base on the given Schema AttributeTypes
    * @param attributeTypes Schema AttributeTypeList
    * @param fields fields value
    * @return parsedFields in the target AttributeTypes
    */
  @throws[AttributeTypeException]
  def parseFields(
      fields: Array[Any],
      attributeTypes: Array[AttributeType]
  ): Array[Any] = {
    fields.indices.map(i => parseField(fields(i), attributeTypes(i))).toArray
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
      field: Any,
      attributeType: AttributeType
  ): Any = {
    if (field == null) return null
    attributeType match {
      case INTEGER   => parseInteger(field)
      case LONG      => parseLong(field)
      case DOUBLE    => parseDouble(field)
      case BOOLEAN   => parseBoolean(field)
      case TIMESTAMP => parseTimestamp(field)
      case STRING    => field.toString
      case BINARY    => field
      case ANY | _   => field
    }
  }

  @throws[AttributeTypeException]
  def parseInteger(fieldValue: Any): Integer = {
    fieldValue match {
      case str: String                => str.trim.toInt
      case int: Integer               => int
      case long: java.lang.Long       => long.toInt
      case double: java.lang.Double   => double.toInt
      case boolean: java.lang.Boolean => if (boolean) 1 else 0
      // Timestamp and Binary are considered to be illegal here.
      case _ =>
        throw new AttributeTypeException(
          s"not able to parse type ${fieldValue.getClass} to Integer: ${fieldValue.toString}"
        )
    }
  }

  @throws[AttributeTypeException]
  def parseLong(fieldValue: Any): java.lang.Long = {
    fieldValue match {
      case str: String                => str.trim.toLong
      case int: Integer               => int.toLong
      case long: java.lang.Long       => long
      case double: java.lang.Double   => double.toLong
      case boolean: java.lang.Boolean => if (boolean) 1L else 0L
      case timestamp: Timestamp       => timestamp.toInstant.toEpochMilli
      // Binary is considered to be illegal here.
      case _ =>
        throw new AttributeTypeException(
          s"not able to parse type ${fieldValue.getClass} to Long: ${fieldValue.toString}"
        )
    }
  }

  @throws[AttributeTypeException]
  def parseTimestamp(fieldValue: Any): Timestamp = {
    val parseError = new AttributeTypeException(
      s"not able to parse type ${fieldValue.getClass} to Timestamp: ${fieldValue.toString}"
    )
    fieldValue match {
      case str: String          => new Timestamp(DateParserUtils.parseDate(str.trim).getTime)
      case long: java.lang.Long => new Timestamp(long)
      case timestamp: Timestamp => timestamp
      case date: java.util.Date => new Timestamp(date.getTime)
      // Integer, Double, Boolean, Binary are considered to be illegal here.
      case _ =>
        throw parseError
    }
  }

  @throws[AttributeTypeException]
  def parseDouble(fieldValue: Any): java.lang.Double = {
    fieldValue match {
      case str: String                => str.trim.toDouble
      case int: Integer               => int.toDouble
      case long: java.lang.Long       => long.toDouble
      case double: java.lang.Double   => double
      case boolean: java.lang.Boolean => if (boolean) 1 else 0
      // Timestamp and Binary are considered to be illegal here.
      case _ =>
        throw new AttributeTypeException(
          s"not able to parse type ${fieldValue.getClass} to Double: ${fieldValue.toString}"
        )
    }
  }

  @throws[AttributeTypeException]
  def parseBoolean(fieldValue: Any): java.lang.Boolean = {
    val parseError = new AttributeTypeException(
      s"not able to parse type ${fieldValue.getClass} to Boolean: ${fieldValue.toString}"
    )
    fieldValue match {
      case str: String =>
        (Try(str.trim.toBoolean) orElse Try(str.trim.toInt == 1))
          .getOrElse(throw parseError)
      case int: Integer               => int != 0
      case long: java.lang.Long       => long != 0
      case double: java.lang.Double   => double != 0
      case boolean: java.lang.Boolean => boolean
      // Timestamp and Binary are considered to be illegal here.
      case _ =>
        throw parseError
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
      fields: Array[Any]
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
  def inferSchemaFromRows(fieldsIterator: Iterator[Array[Any]]): Array[AttributeType] = {
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
  def inferField(fieldValue: Any): AttributeType = {
    tryParseInteger(fieldValue)
  }

  private def tryParseInteger(fieldValue: Any): AttributeType = {
    if (fieldValue == null)
      return INTEGER
    allCatch opt parseInteger(fieldValue) match {
      case Some(_) => INTEGER
      case None    => tryParseLong(fieldValue)
    }
  }

  private def tryParseLong(fieldValue: Any): AttributeType = {
    if (fieldValue == null)
      return LONG
    allCatch opt parseLong(fieldValue) match {
      case Some(_) => LONG
      case None    => tryParseTimestamp(fieldValue)
    }
  }

  private def tryParseTimestamp(fieldValue: Any): AttributeType = {
    if (fieldValue == null)
      return TIMESTAMP
    allCatch opt parseTimestamp(fieldValue) match {
      case Some(_) => TIMESTAMP
      case None    => tryParseDouble(fieldValue)
    }
  }

  private def tryParseDouble(fieldValue: Any): AttributeType = {
    if (fieldValue == null)
      return DOUBLE
    allCatch opt parseDouble(fieldValue) match {
      case Some(_) => DOUBLE
      case None    => tryParseBoolean(fieldValue)
    }
  }

  private def tryParseBoolean(fieldValue: Any): AttributeType = {
    if (fieldValue == null)
      return BOOLEAN
    allCatch opt parseBoolean(fieldValue) match {
      case Some(_) => BOOLEAN
      case None    => tryParseString()
    }
  }

  private def tryParseString(): AttributeType = {
    STRING
  }

  /**
    * InferField when get both typeSofar and tuple string
    * @param attributeType typeSofar
    * @param fieldValue data field to be parsed, original as String field
    * @return inferred AttributeType
    */
  def inferField(attributeType: AttributeType, fieldValue: Any): AttributeType = {
    attributeType match {
      case STRING    => tryParseString()
      case BOOLEAN   => tryParseBoolean(fieldValue)
      case DOUBLE    => tryParseDouble(fieldValue)
      case LONG      => tryParseLong(fieldValue)
      case INTEGER   => tryParseInteger(fieldValue)
      case TIMESTAMP => tryParseTimestamp(fieldValue)
      case BINARY    => tryParseString()
      case _         => tryParseString()
    }
  }

  class AttributeTypeException(msg: String) extends IllegalArgumentException(msg) {}
}
