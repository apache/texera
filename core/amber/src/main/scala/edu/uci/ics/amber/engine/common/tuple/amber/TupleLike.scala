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

  def enforceSchema(tupleLike: TupleLike, schema: Schema): Tuple = {
    enforceSchema(tupleLike.asInstanceOf[SchemaEnforceable], schema)
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
    * Constructs a `Tuple` based on the provided schema and `tupleLike` object.
    *
    * For each attribute in the schema, the function attempts to find a corresponding value
    * in the tuple-like object's field mappings. If a mapping is found, that value is used;
    * otherwise, `null` is used as the attribute value in the built tuple.
    *
    * @param tupleLike An object representing the source of data for the tuple to be built,
    *                  with a mapping from attribute names to their values.
    * @param schema    The schema defining the attributes and their types for the tuple.
    * @return          A new `Tuple` instance built according to the schema and the data provided
    *                  by the `tupleLike` object.
    */
  private def buildTupleWithSchema(tupleLike: MapTupleLike, schema: Schema): Tuple = {
    val builder = Tuple.newBuilder(schema)
    schema.getAttributesScala.foreach { attribute =>
      val value = tupleLike.fieldMappings.getOrElse(attribute.getName, null)
      builder.add(attribute, value)
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
