package edu.uci.ics.amber.engine.common.tuple

import edu.uci.ics.amber.engine.common.tuple.amber.{MapTupleLike, SeqTupleLike, TupleLike}

import scala.util.hashing.MurmurHash3

object ITuple {
  def apply(tupleLike: TupleLike): ITuple = {
    tupleLike match {
      case tuple: ITuple      => tuple
      case like: MapTupleLike => apply(like.fieldMappings.values)
      case like: SeqTupleLike => apply(like.fields)
      case _                  => ???
    }
  }
  def apply(values: Any*): ITuple =
    new ITuple {
      override val fields: Array[Any] = values.toArray

      // dummy value, will reflect real size in Tuple.
      override def inMemSize: Long = 200L

      override def length: Int = fields.length

      override def get(i: Int): Any = fields(i)
    }
}

trait ITuple extends Serializable with SeqTupleLike {
  def inMemSize: Long
  def size: Int = length

  def length: Int

  def apply(i: Int): Any = get(i)

  def get(i: Int): Any

  def getAs[T](i: Int): T = get(i).asInstanceOf[T]
  def isNullAt(i: Int): Boolean = get(i) == null
  def getInstant(i: Int): java.time.Instant = getAs[java.time.Instant](i)
  def getSeq[T](i: Int): Seq[T] = getAs[Seq[T]](i)
  def getList[T](i: Int): List[T] = getSeq[T](i).toList
  def getMap[K, V](i: Int): scala.collection.Map[K, V] = getAs[Map[K, V]](i)
  def getLocalDate(i: Int): java.time.LocalDate = getAs[java.time.LocalDate](i)
  def getDate(i: Int): java.sql.Date = getAs[java.sql.Date](i)
  def getBigDecimal(i: Int): BigDecimal = getAs[BigDecimal](i)
  def getString(i: Int): String = getAs[String](i)
  def getDouble(i: Int): Double = getAnyValAs[Double](i)
  def getFloat(i: Int): Float = getAnyValAs[Float](i)
  def getLong(i: Int): Long = getAnyValAs[Long](i)
  def getInt(i: Int): Int = getAnyValAs[Int](i)
  def getShort(i: Int): Short = getAnyValAs[Short](i)
  def getByte(i: Int): Byte = getAnyValAs[Byte](i)
  def getBoolean(i: Int): Boolean = getAnyValAs[Boolean](i)

  override def hashCode: Int = {
    var n = 0
    var h = MurmurHash3.seqSeed
    val len = length
    while (n < len) {
      h = MurmurHash3.mix(h, apply(n).##)
      n += 1
    }
    MurmurHash3.finalizeHash(h, n)
  }

  def toSeq: Seq[Any] = {
    val n = length
    val values = new Array[Any](n)
    var i = 0
    while (i < n) {
      values.update(i, get(i))
      i += 1
    }
    values.toSeq
  }

  override def equals(o: Any): Boolean = {
    if (!o.isInstanceOf[ITuple]) return false
    val other = o.asInstanceOf[ITuple]

    if (other eq null) return false

    if (length != other.length) {
      return false
    }

    var i = 0
    while (i < length) {
      if (isNullAt(i) != other.isNullAt(i)) {
        return false
      }
      if (!isNullAt(i)) {
        val o1 = get(i)
        val o2 = other.get(i)
        o1 match {
          case b1: Array[Byte] =>
            if (
              !o2.isInstanceOf[Array[Byte]] ||
              !java.util.Arrays.equals(b1, o2.asInstanceOf[Array[Byte]])
            ) {
              return false
            }
          case f1: Float if java.lang.Float.isNaN(f1) =>
            if (!o2.isInstanceOf[Float] || !java.lang.Float.isNaN(o2.asInstanceOf[Float])) {
              return false
            }
          case d1: Double if java.lang.Double.isNaN(d1) =>
            if (!o2.isInstanceOf[Double] || !java.lang.Double.isNaN(o2.asInstanceOf[Double])) {
              return false
            }
          case d1: java.math.BigDecimal if o2.isInstanceOf[java.math.BigDecimal] =>
            if (d1.compareTo(o2.asInstanceOf[java.math.BigDecimal]) != 0) {
              return false
            }
          case _ =>
            if (o1 != o2) {
              return false
            }
        }
      }
      i += 1
    }
    true
  }

  def mkString: String = mkString(",")

  def mkString(sep: String): String = mkString("Tuple[", sep, "]")

  def mkString(start: String, sep: String, end: String): String = {
    val n = length
    val builder = new StringBuilder
    builder.append(start)
    if (n > 0) {
      builder.append(get(0))
      var i = 1
      while (i < n) {
        builder.append(sep)
        builder.append(get(i))
        i += 1
      }
    }
    builder.append(end)
    builder.toString()
  }

  override def toString: String = mkString

  private def getAnyValAs[T <: AnyVal](i: Int): T =
    if (isNullAt(i)) throw new NullPointerException(s"Value at index $i is null")
    else getAs[T](i)

}
