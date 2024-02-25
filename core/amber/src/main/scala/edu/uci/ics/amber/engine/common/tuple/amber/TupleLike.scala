package edu.uci.ics.amber.engine.common.tuple.amber

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.jdk.CollectionConverters.CollectionHasAsScala

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

  def apply(fieldList: java.util.List[Any]): SeqTupleLike = {
    new SeqTupleLike {

      override def inMemSize: Long = ???

      override def fields: Array[Any] = fieldList.asScala.toArray
    }
  }

  def apply(fieldSeq: Any*): SeqTupleLike = {
    new SeqTupleLike {

      override def inMemSize: Long = ???

      override def fields: Array[Any] = fieldSeq.toArray
    }
  }

  /**
    * Transforms a TupleLike object to a Tuple that conforms to a given Schema.
    *
    * @param tupleLike The TupleLike object to be transformed.
    * @param schema The Schema to which the tupleLike object must conform.
    * @return A Tuple that matches the specified schema.
    * @throws RuntimeException if the tupleLike object type is unsupported or invalid for schema enforcement.
    */
  def enforceSchema(tupleLike: SchemaEnforceable, schema: Schema): Tuple = {
    tupleLike match {
      case tTuple: Tuple =>
        assert(
          tTuple.getSchema == schema,
          s"output tuple schema does not match the expected schema! " +
            s"output schema: ${tTuple.getSchema}, " +
            s"expected schema: $schema"
        )
        tTuple
      case map: MapTupleLike =>
        buildTupleWithSchema(map, schema)
      case seq: SeqTupleLike =>
        buildTupleWithSchema(seq, schema)
      case _ =>
        throw new RuntimeException("invalid tuple type, cannot enforce schema")
    }
  }

  /**
    * Constructs a `Tuple` object based on a given schema and a map of field mappings.
    *
    * This method iterates over the field mappings provided by the `tupleLike` object, adding each field to the `Tuple` builder
    * based on the corresponding attribute in the `schema`. The `schema` defines the structure and types of fields allowed in the `Tuple`.
    *
    * @param tupleLike The source of field mappings, where each entry maps a field name to its value.
    * @param schema    The schema defining the structure and types of the `Tuple` to be built.
    * @return A `Tuple` instance that matches the provided schema and contains the data from `tupleLike`.
    */
  private def buildTupleWithSchema(tupleLike: MapTupleLike, schema: Schema): Tuple = {
    val builder = Tuple.newBuilder(schema)
    tupleLike.fieldMappings.foreach {
      case (name, value) =>
        builder.add(schema.getAttribute(name), value)
    }
    builder.build()
  }

  /**
    * Constructs a Tuple object from a sequence of field values
    * according to the specified schema. It asserts that the number
    * of provided fields matches the schema's requirement, every
    * field must also satisfy the field type.
    *
    * @param tupleLike Sequence of field values.
    * @param schema Schema for Tuple construction.
    * @return Tuple constructed according to the schema.
    */
  private def buildTupleWithSchema(tupleLike: SeqTupleLike, schema: Schema): Tuple = {
    val attributes = schema.getAttributes
    val builder = Tuple.newBuilder(schema)
    tupleLike.fields.zipWithIndex.foreach {
      case (value, i) =>
        builder.add(attributes.get(i), value)
    }
    builder.build()
  }
}
