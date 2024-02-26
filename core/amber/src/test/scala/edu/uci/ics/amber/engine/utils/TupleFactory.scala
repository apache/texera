package edu.uci.ics.amber.engine.utils

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

object TupleFactory {
  def mkTuple(fields: Any*): Tuple = {
    val arrayOfObjects: Array[Object] = fields.map(_.asInstanceOf[Object]).toArray
    val schemaBuilder = Schema.newBuilder()
    fields.indices.foreach { i =>
      schemaBuilder.add(new Attribute("field" + i, AttributeType.ANY))
    }
    val schema = schemaBuilder.build()
    Tuple.newBuilder(schema).addSequentially(arrayOfObjects).build()
  }

  def mkTupleWithSchema(schema: Schema, fields: Any*): Tuple = {
    val arrayOfObjects: Array[Object] = fields.map(_.asInstanceOf[Object]).toArray
    Tuple.newBuilder(schema).addSequentially(arrayOfObjects).build()
  }

}
