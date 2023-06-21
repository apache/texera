package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.amberfield.FieldType

class TupleMetadata(val fields: Array[(String, FieldType.Value)]) {
  def this(fieldsCount: Int) = {
    this(Array.tabulate(fieldsCount)(i => ("_c" + i, FieldType.String)))
  }
  def this(fieldTypes: Array[FieldType.Value]) = {
    this(Array.tabulate(fieldTypes.length)(i => ("_c" + i, fieldTypes(i))))
  }
  def this(fieldNames: Array[String]) = {
    this(Array.tabulate(fieldNames.length)(i => (fieldNames(i), FieldType.String)))
  }
  override def toString = s"TupleMataData: ${fields.mkString(", ")}"

  //use 'lazy val' instead of 'def' to make sure the following two only run once and in a lazy manner
  lazy val fieldTypes: Array[FieldType.Value] = fields.map(i => i._2)
  lazy val names: Array[String] = fields.map(i => i._1)
}

class TableMetadata(val name: String, val tupleMetadata: TupleMetadata) {
  override def toString = s"TableMataData:\n\tName: $name \n\t$tupleMetadata"
}
