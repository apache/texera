package edu.uci.ics.amber.engine.common.tuple.amber

import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.jdk.CollectionConverters.CollectionHasAsScala

sealed trait FieldArray {
  def getFields: Array[Any]
}

sealed trait TupleLike extends FieldArray {
  def inMemSize: Long = 0L
}

trait SchemaEnforceable {
  def enforceSchema(schema: Schema): Tuple
}

trait InternalMarker extends TupleLike {
  override def getFields: Array[Any] = Array.empty
}

final case class FinalizePort(portId: PortIdentity, input: Boolean) extends InternalMarker
final case class FinalizeExecutor() extends InternalMarker

trait SeqTupleLike extends TupleLike with SchemaEnforceable {
  override def inMemSize: Long = ???

  /**
    * Constructs a Tuple object from a sequence of field values
    * according to the specified schema. It asserts that the number
    * of provided fields matches the schema's requirement, every
    * field must also satisfy the field type.
    *
    * @param schema    Schema for Tuple construction.
    * @return Tuple constructed according to the schema.
    */
  override def enforceSchema(schema: Schema): Tuple = {
    val attributes = schema.getAttributes
    val builder = Tuple.builder(schema)
    getFields.zipWithIndex.foreach {
      case (value, i) =>
        builder.add(attributes(i), value)
    }
    builder.build()
  }

}

trait MapTupleLike extends SeqTupleLike with SchemaEnforceable {
  override def inMemSize: Long = ???
  def fieldMappings: Map[String, Any]
  override def getFields: Array[Any] = fieldMappings.values.toArray

  /**
    * Constructs a `Tuple` based on the provided schema and `tupleLike` object.
    *
    * For each attribute in the schema, the function attempts to find a corresponding value
    * in the tuple-like object's field mappings. If a mapping is found, that value is used;
    * otherwise, `null` is used as the attribute value in the built tuple.
    *
    * @param schema    The schema defining the attributes and their types for the tuple.
    * @return A new `Tuple` instance built according to the schema and the data provided
    *         by the `tupleLike` object.
    */
  override def enforceSchema(schema: Schema): Tuple = {
    val builder = Tuple.builder(schema)
    schema.getAttributes.foreach { attribute =>
      val value = fieldMappings.getOrElse(attribute.getName, null)
      builder.add(attribute, value)
    }
    builder.build()
  }
}

object TupleLike {

  // Implicit evidence markers for different types
  trait NotAnIterable[A]

  // Provide a low-priority implicit evidence for all types that are not Iterable
  trait LowPriorityNotAnIterableImplicits {
    implicit def defaultNotAnIterable[A]: NotAnIterable[A] = new NotAnIterable[A] {}
  }

  // Object to hold the implicits
  object NotAnIterable extends LowPriorityNotAnIterableImplicits {
    // Prioritize this implicit for Strings, allowing them explicitly
    implicit object StringIsNotAnIterable extends NotAnIterable[String]

    // Ensure Iterable types do not have an implicit NotAnIterable available
    // This is a way to "exclude" Iterable types by not providing an implicit instance for them
    implicit def iterableIsNotAnIterable[C[_] <: Iterable[A], A]: NotAnIterable[C[A]] =
      throw new RuntimeException("Iterable types are not allowed")
  }

  def apply(mappings: Map[String, Any]): MapTupleLike = {
    new MapTupleLike {
      override val fieldMappings: Map[String, Any] = mappings
    }
  }

  def apply(mappings: Iterable[(String, Any)]): MapTupleLike = {
    new MapTupleLike {
      override val fieldMappings: Map[String, Any] = mappings.toMap
    }
  }

  def apply(mappings: (String, Any)*): MapTupleLike = {
    new MapTupleLike {
      override val fieldMappings: Map[String, Any] = mappings.toMap
    }
  }

  def apply(fieldList: java.util.List[Any]): SeqTupleLike = {
    new SeqTupleLike {
      override val getFields: Array[Any] = fieldList.asScala.toArray
    }
  }

  def apply[T: NotAnIterable](fieldSeq: T*)(implicit ev: NotAnIterable[_] = null): SeqTupleLike = {
    new SeqTupleLike {
      override val getFields: Array[Any] = fieldSeq.toArray
    }
  }

  def apply[T <: Any](fieldIter: Iterable[T]): SeqTupleLike = {
    new SeqTupleLike {
      override val getFields: Array[Any] = fieldIter.toArray
    }
  }

  def apply(array: Array[Any]): SeqTupleLike = {
    new SeqTupleLike {
      override val getFields: Array[Any] = array
    }
  }

}
