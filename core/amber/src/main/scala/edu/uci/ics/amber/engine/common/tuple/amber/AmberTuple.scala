package edu.uci.ics.amber.engine.common.tuple.amber

import edu.uci.ics.amber.engine.common.amberfield.FieldType
import edu.uci.ics.amber.engine.common.tuple.ITuple

@Deprecated
class AmberTuple(val data: Array[Any]) extends ITuple {
  def this(fields: Array[String], fieldTypes: Array[FieldType.Value]) = {
    this({
      var i = 0
      val limit = Math.min(fields.length, fieldTypes.length)
      val result = new Array[Any](limit)
      while (i < limit) {
        result(i) = fieldTypes(i) match {
          case FieldType.Short      => fields(i).toShort
          case FieldType.Int        => fields(i).toInt
          case FieldType.Boolean    => fields(i).toBoolean
          case FieldType.Char       => fields(i)(0)
          case FieldType.Double     => fields(i).toDouble
          case FieldType.Byte       => fields(i).toByte
          case FieldType.Float      => fields(i).toFloat
          case FieldType.Long       => fields(i).toLong
          case FieldType.String     => fields(i)
          case FieldType.BigInt     => BigInt(fields(i))
          case FieldType.BigDecimal => BigDecimal(fields(i))
          case FieldType.Other      => fields(i)
        }
        i += 1
      }
      result
    })
  }

  override def length: Int = data.length

  override def get(i: Int): Any = data(i)

  override def toArray(): Array[Any] = data
}
