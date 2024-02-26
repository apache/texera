package edu.uci.ics.amber.engine.common.tuple.amber

sealed trait FieldArray {
  def fields: Array[Any]
}

sealed trait TupleLike extends FieldArray {
  def inMemSize: Long = 0L
}

trait SchemaEnforceable

trait SpecialTupleLike extends TupleLike

trait SeqTupleLike extends TupleLike with SchemaEnforceable

trait MapTupleLike extends SeqTupleLike with SchemaEnforceable {
  def fieldMappings: Map[String, Any]

  override val fields: Array[Any] = fieldMappings.values.toArray
}

object TupleLike {

  def apply(mappings: Map[String, Any]): MapTupleLike = {
    new MapTupleLike {

      override def inMemSize: Long = ???

      override def fieldMappings: Map[String, Any] = mappings
    }
  }
  def apply(mappings: (String, Any)*): MapTupleLike = {
    new MapTupleLike {
      override def fieldMappings: Map[String, Any] = mappings.toMap

      override def inMemSize: Long = ???
    }
  }

  def apply(fieldSeq: Any*): SeqTupleLike = {
    new SeqTupleLike {

      override def inMemSize: Long = ???

      override def fields: Array[Any] = fieldSeq.toArray
    }
  }
}
