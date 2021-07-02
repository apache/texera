package edu.uci.ics.texera.workflow.operators.pythonUDF

import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.AttributeTypeException
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, AttributeTypeUtils}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.TimeUnit.NANOSECOND
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.ArrowType.PrimitiveType



object ArrowTypeUtils {

  @throws[AttributeTypeException]
  def fromAttributeType(srcType: AttributeType): PrimitiveType = {
    srcType match {
      case AttributeType.INTEGER =>
        new ArrowType.Int(32, true)

      case AttributeType.LONG =>
        new ArrowType.Int(64, true)

      case AttributeType.DOUBLE =>
        new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)

      case AttributeType.BOOLEAN =>
        ArrowType.Bool.INSTANCE

      case AttributeType.TIMESTAMP =>
        new ArrowType.Timestamp(NANOSECOND, "UTC")

      case AttributeType.STRING | AttributeType.ANY =>
        ArrowType.Utf8.INSTANCE
      case _ =>
        throw new AttributeTypeUtils.AttributeTypeException("Unexpected value: " + srcType)
    }
  }

  def toAttributeType(srcType: ArrowType): AttributeType = {
    srcType match {
      case int: ArrowType.Int =>
        int.getBitWidth match {
          case 16 | 32 =>
            AttributeType.INTEGER

          case 64 | _ =>
            AttributeType.LONG
        }
      case _: ArrowType.Bool =>
        AttributeType.BOOLEAN

      case _: ArrowType.FloatingPoint =>
        AttributeType.DOUBLE

      case _: ArrowType.Utf8 =>
        AttributeType.STRING

      case _: ArrowType.Timestamp =>
        AttributeType.TIMESTAMP

      case _ =>
        throw new AttributeTypeUtils.AttributeTypeException(
          "Unexpected value: " + srcType.getTypeID
        )
    }
  }

}
