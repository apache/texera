package edu.uci.ics.amber.engine.common.tuple.advanced

import java.nio.ByteBuffer

import edu.uci.ics.amber.engine.common.amberfield.FieldType
import edu.uci.ics.amber.engine.common.tuple.amber.AmberTuple

//WARN: only use it when you really need save space for large & complex(mixed data types) tuples (~70% compression ratio)

class CompactTuple private (val data: Array[Byte]) extends Serializable {
  def decompress(fieldTypes: Array[FieldType.Value]): AmberTuple = {
    var counter = 0
    var cursor = math.ceil(fieldTypes.length / 8f).toInt
    val decompressed = new Array[Any](fieldTypes.length)
    val buffer = ByteBuffer.wrap(data)
    while (counter < fieldTypes.length) {
      decompressed(counter) = if ((data(counter >> 3) >> counter % 8 & 1) == 0) {
        fieldTypes(counter) match {
          case FieldType.Boolean => cursor += 1; if (buffer.get(cursor - 1) != 0) true else false
          case FieldType.Byte    => cursor += 1; buffer.get(cursor - 1)
          case FieldType.Short   => cursor += 2; buffer.getShort(cursor - 2)
          case FieldType.Char    => cursor += 2; buffer.getChar(cursor - 2)
          case FieldType.Int     => cursor += 4; buffer.getInt(cursor - 4)
          case FieldType.Float   => cursor += 4; buffer.getFloat(cursor - 4)
          case FieldType.Double  => cursor += 8; buffer.getDouble(cursor - 8)
          case FieldType.Long    => cursor += 8; buffer.getLong(cursor - 8)
          case FieldType.String =>
            val len = buffer.getInt(cursor); cursor += 4 + len; new String(data, cursor - len, len)
          case FieldType.BigInt =>
            val len = buffer.getInt(cursor); cursor += 4 + len;
            BigInt(data.slice(cursor - len, cursor))
          case FieldType.BigDecimal =>
            val len = buffer.getInt(cursor); cursor += 4 + len;
            BigDecimal(new String(data, cursor - len, len))
          case FieldType.Other =>
            val len = buffer.getInt(cursor); cursor += 4 + len; new String(data, cursor - len, len)
        }
      } else {
        None
      }
      counter += 1
    }
    new AmberTuple(decompressed)
  }
}

object CompactTuple {
  def apply(amberTuple: AmberTuple): CompactTuple = {
    val len = amberTuple.data.length
    val data = amberTuple.data
    val nullIndicatorSize = math.ceil(len / 8f).toInt
    var compressed = new Array[Byte](nullIndicatorSize)
    var counter = 0
    while (counter < len) {
      if (data(counter) == null) {
        compressed(counter >> 3) = (compressed(counter >> 3) | (1 << counter % 8)).toByte
      } else {
        val temp: Array[Byte] = data(counter) match {
          case i: Boolean => Array(if (i) 1.toByte else 0.toByte)
          case i: Byte    => Array(i)
          case i: Char    => ByteBuffer.allocate(2).putChar(i).array()
          case i: Short   => ByteBuffer.allocate(2).putShort(i).array()
          case i: Int     => ByteBuffer.allocate(4).putInt(i).array()
          case i: Float   => ByteBuffer.allocate(4).putFloat(i).array()
          case i: Double  => ByteBuffer.allocate(8).putDouble(i).array()
          case i: Long    => ByteBuffer.allocate(8).putLong(i).array()
          case i: String =>
            val arr = i.getBytes(); ByteBuffer.allocate(4).putInt(arr.length).array() ++ arr
          case i: BigInt =>
            val arr = i.toByteArray; ByteBuffer.allocate(4).putInt(arr.length).array() ++ arr
          case i: BigDecimal =>
            val arr = i.toString().getBytes();
            ByteBuffer.allocate(4).putInt(arr.length).array() ++ arr
        }
        compressed ++= temp
      }
      counter += 1
    }
    new CompactTuple(compressed)
  }
}
