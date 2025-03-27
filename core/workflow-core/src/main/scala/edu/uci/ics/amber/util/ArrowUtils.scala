package edu.uci.ics.amber.util

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.AttributeTypeException
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, AttributeTypeUtils, Schema, Tuple}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.TimeUnit.MILLISECOND
import org.apache.arrow.vector.types.pojo.ArrowType.PrimitiveType
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.arrow.vector.{
  BigIntVector,
  BitVector,
  FieldVector,
  Float8Vector,
  IntVector,
  TimeStampVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.complex.LargeListVector
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}

import java.nio.charset.StandardCharsets
import java.util
import scala.collection.convert.ImplicitConversions.`seq AsJavaList`
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.language.implicitConversions

object ArrowUtils extends LazyLogging {

  // Create a single allocator for the entire utility
  private val allocator: BufferAllocator = new RootAllocator()

  implicit def bool2int(b: Boolean): Int = if (b) 1 else 0

  /**
    * Reads a row of the given Arrow Vectors into a Texera.Tuple
    * e.g.,
    * rowIndex  IntVector BigIntVector  BooleanVector
    * 0         1         100L          true
    *
    * the row at rowIndex 0 can be converted into `Tuple[1, 100L, true]`
    *
    * @param rowIndex         The row index of the target row to be converted in the Vectors.
    * @param vectorSchemaRoot The root of the Vectors that stores the Arrow Fields. It contains multiple Vectors.
    * @return
    */
  def getTexeraTuple(
      rowIndex: Int,
      vectorSchemaRoot: VectorSchemaRoot
  ): Tuple = {
    val arrowSchema = vectorSchemaRoot.getSchema
    val schema = toTexeraSchema(arrowSchema)

    val builder = Tuple.builder(schema)
    val fieldVectors = vectorSchemaRoot.getFieldVectors

    (0 until fieldVectors.size()).foreach { i =>
      val fieldVector = fieldVectors.get(i)
      val arrowType = fieldVector.getField.getFieldType.getType
      val attribute = schema.getAttributes.get(i)

      try {
        val value = arrowType match {
          case _: ArrowType.LargeList =>
            fieldVector match {
              case largeListVector: LargeListVector =>
                if (largeListVector.isNull(rowIndex)) {
                  null
                } else {
                  // Get the inner value vector (should be binary type for BINARY AttributeType)
                  val innerVector = largeListVector.getDataVector

                  // Get start/end indexes for this row's list - LargeListVector uses Long offsets
                  val startIdx = largeListVector.getOffsetBuffer.getLong(rowIndex * 8).toInt
                  val endIdx = largeListVector.getOffsetBuffer.getLong((rowIndex + 1) * 8).toInt

                  // Convert to List[ByteBuffer]
                  (startIdx until endIdx).map { j =>
                    if (innerVector.isNull(j)) {
                      java.nio.ByteBuffer.allocate(0)
                    } else {
                      val bytes = innerVector.getObject(j).asInstanceOf[Array[Byte]]
                      java.nio.ByteBuffer.wrap(bytes)
                    }
                  }.toList
                }

              case _ =>
                logger.warn(s"Unsupported list vector type: ${fieldVector.getClass.getName}")
                null
            }

          case _ =>
            // For other types, get the object and parse it using the AttributeType
            val rawValue = fieldVector.getObject(rowIndex)
            val attributeType = toAttributeType(arrowType)
            AttributeTypeUtils.parseField(rawValue, attributeType)
        }

        builder.add(attribute, value)
      } catch {
        case e: Exception =>
          logger.warn("Caught error during parsing Arrow value back to Texera value", e)
          builder.add(attribute, null)
      }
    }

    builder.build()
  }

  /**
    * Converts an Arrow Schema into Texera Schema.
    *
    * @param arrowSchema The Arrow Schema to be converted.
    * @return A Texera Schema.
    */
  def toTexeraSchema(arrowSchema: org.apache.arrow.vector.types.pojo.Schema): Schema =
    Schema(
      arrowSchema.getFields.asScala.map { field =>
        new Attribute(field.getName, toAttributeType(field.getType))
      }.toList
    )

  /**
    * Converts an ArrowType into an AttributeType.
    *
    * @param srcType the ArrowType to be converted.
    * @throws AttributeTypeException if the type cannot be converted.
    * @return An AttributeType.
    */
  @throws[AttributeTypeException]
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

      case _: ArrowType.Timestamp =>
        AttributeType.TIMESTAMP

      case _: ArrowType.Utf8 =>
        AttributeType.STRING

      case _: ArrowType.LargeList =>
        AttributeType.BINARY

      case _ =>
        throw new AttributeTypeUtils.AttributeTypeException(
          "Unexpected value: " + srcType.getTypeID
        )
    }
  }

  def appendTexeraTuple(tuple: Tuple, vectorSchemaRoot: VectorSchemaRoot): Unit = {
    val currentRowCount = vectorSchemaRoot.getRowCount
    val nextRowIndex = currentRowCount
    setTexeraTuple(tuple, nextRowIndex, vectorSchemaRoot)
  }

  /**
    * Writes a Texera.Tuple into a row of the Arrow Vectors. It will overwrite the data on the
    * target row of the Vectors.
    *
    * @param tuple            A Texera.Tuple.
    * @param index            The row index in the Vectors to be replaced.
    * @param vectorSchemaRoot The root of the Vectors that stores the Arrow Fields. It contains
    *                         multiple Vectors.
    */
  def setTexeraTuple(tuple: Tuple, index: Int, vectorSchemaRoot: VectorSchemaRoot): Unit = {
    val arrowSchema = vectorSchemaRoot.getSchema
    val arrowFields = arrowSchema.getFields.asScala.toList

    for (i <- arrowFields.indices) {
      val vector: FieldVector = vectorSchemaRoot.getVector(i)
      val value = tuple.getField[AnyRef](i)
      val isNull = value == null
      arrowFields.apply(i).getFieldType.getType match {
        case _: ArrowType.Int =>
          vector.getField.getFieldType.getType.asInstanceOf[ArrowType.Int].getBitWidth match {
            case 16 | 32 =>
              vector
                .asInstanceOf[IntVector]
                .setSafe(index, !isNull, if (isNull) 0 else value.asInstanceOf[Int])

            case 64 | _ =>
              vector
                .asInstanceOf[BigIntVector]
                .setSafe(index, !isNull, if (isNull) 0 else value.asInstanceOf[Long])
          }

        case _: ArrowType.Bool =>
          vector
            .asInstanceOf[BitVector]
            .setSafe(index, !isNull, if (isNull) 0 else value.asInstanceOf[Boolean])

        case _: ArrowType.FloatingPoint =>
          vector
            .asInstanceOf[Float8Vector]
            .setSafe(index, !isNull, if (isNull) 0 else value.asInstanceOf[Double])

        case _: ArrowType.Timestamp =>
          vector
            .asInstanceOf[TimeStampVector]
            .setSafe(
              index,
              !isNull,
              if (isNull) 0L
              else
                AttributeTypeUtils
                  .parseField(value, AttributeType.LONG)
                  .asInstanceOf[Long]
            )

        case _: ArrowType.Utf8 =>
          if (isNull) vector.asInstanceOf[VarCharVector].setNull(index)
          else
            vector
              .asInstanceOf[VarCharVector]
              .setSafe(index, value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
        case _: ArrowType.LargeList =>
          (isNull, vector) match {
            case (true, largeListVector: LargeListVector) =>
              largeListVector.setNull(index)

            case (true, _) =>
              logger.warn(
                s"Unsupported list vector type for null value: ${vector.getClass.getName}"
              )

            case (false, largeListVector: LargeListVector) =>
              value match {
                case bufferList: List[_]
                    if bufferList.isEmpty || bufferList.head.isInstanceOf[java.nio.ByteBuffer] =>
                  val writer = largeListVector.getWriter
                  writer.setPosition(index)
                  writer.startList()

                  // Process ByteBuffers in a more functional way
                  if (bufferList.nonEmpty) {
                    bufferList.asInstanceOf[List[java.nio.ByteBuffer]].foreach { buffer =>
                      if (buffer == null) {
                        // For null elements, write an empty byte array
                        val arrowBuf = allocator.buffer(0)
                        try {
                          writer.writeVarBinary(0, 0, arrowBuf)
                        } finally {
                          arrowBuf.close() // Ensure buffer is released
                        }
                      } else {
                        val bytes = Array.ofDim[Byte](buffer.remaining())
                        buffer.duplicate().get(bytes)

                        // Create temporary Arrow buffer to hold binary data
                        val arrowBuf = allocator.buffer(bytes.length)
                        try {
                          arrowBuf.writeBytes(bytes)
                          writer.writeVarBinary(0, bytes.length, arrowBuf)
                        } finally {
                          arrowBuf.close() // Ensure buffer is released
                        }
                      }
                    }
                  }

                  writer.endList()

                case other =>
                  throw new AttributeTypeUtils.AttributeTypeException(
                    s"Cannot convert ${other.getClass.getName} to list of binary data"
                  )
              }

            case (false, _) =>
              logger.warn(s"Unsupported list vector type: ${vector.getClass.getName}")
              throw new AttributeTypeUtils.AttributeTypeException(
                s"Cannot write to unsupported list vector type: ${vector.getClass.getName}"
              )
          }
      }
    }

    vectorSchemaRoot.setRowCount(vectorSchemaRoot.getRowCount + 1)
  }

  /**
    * Converts an Amber schema into Arrow schema.
    *
    * @param schema The Texera Schema.
    * @return An Arrow Schema.
    */
  def fromTexeraSchema(schema: Schema): org.apache.arrow.vector.types.pojo.Schema = {
    val arrowFields = new util.ArrayList[Field]

    for (amberAttribute <- schema.getAttributes) {
      val name = amberAttribute.getName
      val attributeType = amberAttribute.getType

      val field = attributeType match {
        case AttributeType.BINARY =>
          // For BINARY type, create a LargeList field with Binary element type
          val childField = Field.nullablePrimitive("element", new ArrowType.Binary())
          new Field(
            name,
            FieldType.nullable(new ArrowType.LargeList()),
            util.Arrays.asList(childField)
          )
        case _ =>
          Field.nullablePrimitive(name, fromAttributeTypeToPrimitive(attributeType))
      }

      arrowFields.add(field)
    }
    new org.apache.arrow.vector.types.pojo.Schema(arrowFields)
  }

  /**
    * Converts an AttributeType into a primitive ArrowType.
    *
    * @param srcType The AttributeType to be converted.
    * @throws AttributeTypeException if the type cannot be converted to a primitive type.
    * @return A PrimitiveType (a subtype of ArrowType)
    */
  @throws[AttributeTypeException]
  def fromAttributeTypeToPrimitive(srcType: AttributeType): PrimitiveType = {
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
        new ArrowType.Timestamp(MILLISECOND, "UTC")

      case AttributeType.STRING | AttributeType.ANY =>
        ArrowType.Utf8.INSTANCE

      case _ =>
        throw new AttributeTypeUtils.AttributeTypeException("Unexpected value: " + srcType)
    }
  }
}
