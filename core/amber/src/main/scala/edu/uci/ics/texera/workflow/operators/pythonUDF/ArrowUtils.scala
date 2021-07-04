package edu.uci.ics.texera.workflow.operators.pythonUDF

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.AttributeTypeException
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  AttributeTypeUtils,
  Schema
}
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.TimeUnit.NANOSECOND
import org.apache.arrow.vector.types.pojo.ArrowType.PrimitiveType
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}

import java.nio.charset.StandardCharsets
import java.util
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`list asScalaBuffer`
import scala.language.implicitConversions
object ArrowUtils {

  implicit def bool2int(b: Boolean): Int = if (b) 1 else 0

  /**
    * Reads a row of the given Arrow Vectors into a Texera.Tuple
    *    e.g.,
    *    rowIndex  IntVector BigIntVector  BooleanVector
    *    0         1         100L          true
    *
    *    the row at rowIndex 0 can be converted into `Tuple[1, 100L, true]`
    *
    * @param rowIndex The row index of the target row to be converted in the Vectors.
    * @param vectorSchemaRoot The root of the Vectors that stores the Arrow Fields. It contains multiple Vectors.
    * @return
    */
  def getTexeraTuple(
      rowIndex: Int,
      vectorSchemaRoot: VectorSchemaRoot
  ): Tuple = {
    val arrowSchema = vectorSchemaRoot.getSchema
    val schema = toTexeraSchema(arrowSchema)

    Tuple
      .newBuilder(schema)
      .addSequentially(
        vectorSchemaRoot.getFieldVectors
          .map((fieldVector: FieldVector) => {
            val value: AnyRef = fieldVector.getObject(rowIndex)
            try {
              val arrowType = fieldVector.getField.getFieldType.getType
              val attributeType = toAttributeType(arrowType)
              AttributeTypeUtils.parseField(value, attributeType)

            } catch {
              case e: Exception =>
                e.printStackTrace()
                null
            }

          })
          .toArray
          .asInstanceOf[Array[AnyRef]]
      )
      .build()

  }

  /**
    * Converts an Arrow Schema into Texera Schema.
    *
    * @param arrowSchema The Arrow Schema to be converted.
    * @return A Texera Schema.
    */
  def toTexeraSchema(arrowSchema: org.apache.arrow.vector.types.pojo.Schema): Schema =
    Schema
      .newBuilder()
      .add(
        arrowSchema.getFields.toIterable
          .map(field => new Attribute(field.getName, toAttributeType(field.getType)))
          .asJava
      )
      .build()

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
    val arrowFields = arrowSchema.getFields.toList

    for (i <- arrowFields.indices) {
      val vector = vectorSchemaRoot.getVector(i)
      val value = tuple.get(i)
      val isNull = value == null
      arrowFields.apply(i).getFieldType.getType match {
        case _: ArrowType.Int =>
          vector.getField.getFieldType.getType.asInstanceOf[ArrowType.Int].getBitWidth match {
            case 16 | 32 =>
              vector
                .asInstanceOf[IntVector]
                .set(index, !isNull, if (isNull) 0 else value.asInstanceOf[Int])

            case 64 | _ =>
              vector
                .asInstanceOf[BigIntVector]
                .set(index, !isNull, if (isNull) 0 else value.asInstanceOf[Long])
          }

        case _: ArrowType.Bool =>
          vector
            .asInstanceOf[BitVector]
            .set(index, !isNull, if (isNull) 0 else value.asInstanceOf[Boolean])

        case _: ArrowType.FloatingPoint =>
          vector
            .asInstanceOf[Float8Vector]
            .set(index, !isNull, if (isNull) 0 else value.asInstanceOf[Double])

        case _: ArrowType.Timestamp =>
          vector
            .asInstanceOf[TimeStampVector]
            .set(
              index,
              !isNull,
              if (isNull) 0L
              else AttributeTypeUtils.parseField(value, AttributeType.LONG).asInstanceOf[Long]
            )

        case _: ArrowType.Utf8 =>
          if (isNull) vector.asInstanceOf[VarCharVector].setNull(index)
          else
            vector
              .asInstanceOf[VarCharVector]
              .set(index, value.asInstanceOf[String].getBytes(StandardCharsets.UTF_8))

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
      val field = Field.nullablePrimitive(name, fromAttributeType(amberAttribute.getType))
      arrowFields.add(field)
    }
    new org.apache.arrow.vector.types.pojo.Schema(arrowFields)
  }

  /**
    * Converts an AttributeType into an ArrowType (PrimitiveType).
    *
    * @param srcType The AttributeType to be converted.
    * @throws AttributeTypeException if the type cannot be converted.
    * @return A PrimitiveType, a type of ArrowType, does not handle complex data.
    */
  @throws[AttributeTypeException]
  def fromAttributeType(srcType: AttributeType): PrimitiveType = {
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
        new ArrowType.Timestamp(NANOSECOND, "UTC")

      case AttributeType.STRING | AttributeType.ANY =>
        ArrowType.Utf8.INSTANCE
      case _ =>
        throw new AttributeTypeUtils.AttributeTypeException("Unexpected value: " + srcType)
    }
  }

}
