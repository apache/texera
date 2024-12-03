package edu.uci.ics.amber.util

import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.AttributeTypeException
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.{ArrowType, Field}
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp
import scala.jdk.CollectionConverters.IterableHasAsJava
class ArrowUtilsSpec extends AnyFlatSpec {

  val unsignedShortInt = new ArrowType.Int(16, false)
  val signedShortInt = new ArrowType.Int(16, true)
  val unsignedInt = new ArrowType.Int(32, false)
  val signedInt = new ArrowType.Int(32, true)
  val unsignedLongInt = new ArrowType.Int(64, false)
  val signedLongInt = new ArrowType.Int(64, true)
  val boolean: ArrowType.Bool = ArrowType.Bool.INSTANCE
  val float = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
  val double = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
  val half = new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)
  val timestamp = new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")
  val string: ArrowType.Utf8 = ArrowType.Utf8.INSTANCE

  val texeraSchema: Schema = Schema
    .builder()
    .add("test-1", AttributeType.INTEGER)
    .add("test-2", AttributeType.LONG)
    .add("test-3", AttributeType.BOOLEAN)
    .add("test-4", AttributeType.DOUBLE)
    .add("test-5", AttributeType.TIMESTAMP)
    .add("test-6", AttributeType.STRING)
    .build()

  val arrowSchema: org.apache.arrow.vector.types.pojo.Schema =
    new org.apache.arrow.vector.types.pojo.Schema(
      Array(
        Field.nullablePrimitive("test-1", signedInt),
        Field.nullablePrimitive("test-2", signedLongInt),
        Field.nullablePrimitive("test-3", boolean),
        Field.nullablePrimitive("test-4", double),
        Field.nullablePrimitive("test-5", timestamp),
        Field.nullablePrimitive("test-6", string)
      ).toList.asJava
    )

  behavior of "ArrowUtils"

  it should "convert to AttributeTypes correctly" in {
    assert(ArrowUtils.toAttributeType(unsignedShortInt) == AttributeType.INTEGER)
    assert(ArrowUtils.toAttributeType(signedShortInt) == AttributeType.INTEGER)
    assert(ArrowUtils.toAttributeType(unsignedInt) == AttributeType.INTEGER)
    assert(ArrowUtils.toAttributeType(signedInt) == AttributeType.INTEGER)
    assert(ArrowUtils.toAttributeType(unsignedLongInt) == AttributeType.LONG)
    assert(ArrowUtils.toAttributeType(signedLongInt) == AttributeType.LONG)

    assert(ArrowUtils.toAttributeType(boolean) == AttributeType.BOOLEAN)

    assert(ArrowUtils.toAttributeType(float) == AttributeType.DOUBLE)
    assert(ArrowUtils.toAttributeType(double) == AttributeType.DOUBLE)
    assert(ArrowUtils.toAttributeType(half) == AttributeType.DOUBLE)

    assert(ArrowUtils.toAttributeType(timestamp) == AttributeType.TIMESTAMP)
    assert(ArrowUtils.toAttributeType(timestamp) == AttributeType.TIMESTAMP)

    assert(ArrowUtils.toAttributeType(string) == AttributeType.STRING)

  }

  it should "convert from AttributeType correctly" in {
    assert(ArrowUtils.fromAttributeType(AttributeType.INTEGER) == signedInt)
    assert(ArrowUtils.fromAttributeType(AttributeType.LONG) == signedLongInt)
    assert(ArrowUtils.fromAttributeType(AttributeType.BOOLEAN) == boolean)
    assert(ArrowUtils.fromAttributeType(AttributeType.DOUBLE) == double)
    assert(ArrowUtils.fromAttributeType(AttributeType.TIMESTAMP) == timestamp)
    assert(ArrowUtils.fromAttributeType(AttributeType.STRING) == string)

    // a special case that's not symmetric, AttributeType.ANY will be converted to ArrowType.Utf8
    // but not the other way around.
    assert(ArrowUtils.fromAttributeType(AttributeType.ANY) == string)

  }

  it should "raise AttributeTypeException when converting unsupported types" in {
    assertThrows[AttributeTypeException] {
      ArrowUtils.toAttributeType(new ArrowType.Null)
    }

    assertThrows[AttributeTypeException] {
      ArrowUtils.toAttributeType(new ArrowType.Date(DateUnit.DAY))
    }

    assertThrows[AttributeTypeException] {
      ArrowUtils.toAttributeType(new ArrowType.Map(true))
    }

    assertThrows[AttributeTypeException] {
      ArrowUtils.toAttributeType(new ArrowType.List)
    }

    assertThrows[AttributeTypeException] {
      ArrowUtils.toAttributeType(new ArrowType.Interval(IntervalUnit.DAY_TIME))
    }
  }

  it should "convert to Texera Schema correctly" in {

    assert(ArrowUtils.toTexeraSchema(arrowSchema) == texeraSchema)

  }

  it should "convert from Texera Schema correctly" in {

    assert(ArrowUtils.fromTexeraSchema(texeraSchema) == arrowSchema)

  }

  it should "set Arrow Fields from Texera Tuple correctly" in {

    val tuple = Tuple
      .builder(texeraSchema)
      .addSequentially(
        Array(
          Int.box(2),
          Long.box(1L),
          Boolean.box(true),
          Double.box(1.1),
          new Timestamp(10000L),
          "hello world"
        )
      )
      .build()
    val allocator: BufferAllocator = new RootAllocator()
    val vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    vectorSchemaRoot.allocateNew()

    val rowCount = vectorSchemaRoot.getRowCount
    val index = 1

    // set Tuple into the Vectors
    ArrowUtils.setTexeraTuple(tuple, index, vectorSchemaRoot)

    assert(vectorSchemaRoot.getVector(0).getObject(index).asInstanceOf[Int] == 2)
    assert(vectorSchemaRoot.getVector(1).getObject(index).asInstanceOf[Long] == 1L)
    assert(vectorSchemaRoot.getVector(2).getObject(index).asInstanceOf[Boolean] == true)
    assert(vectorSchemaRoot.getVector(3).getObject(index).asInstanceOf[Double] == 1.1)

    // the arrow storage type of timestamp is Long
    assert(vectorSchemaRoot.getVector(4).getObject(index).asInstanceOf[Long] == 10000L)

    // the arrow storage type of string is Text
    assert(
      vectorSchemaRoot.getVector(5).getObject(index) ==
        new org.apache.arrow.vector.util.Text("hello world")
    )

    // should not have more vectors created.
    assertThrows[IllegalArgumentException](vectorSchemaRoot.getVector(7).getObject(index))

    // other fields at other untouched indices should be null
    assert(vectorSchemaRoot.getVector(0).getObject(index + 1) == null)

    // now the rowCount should be incremented by 1
    assert(vectorSchemaRoot.getRowCount == rowCount + 1)

  }

  it should "get Texera Tuple from Arrow Fields correctly" in {

    val tuple = Tuple
      .builder(texeraSchema)
      .addSequentially(
        Array(
          Int.box(2),
          Long.box(1L),
          Boolean.box(true),
          Double.box(1.1),
          new Timestamp(10000L),
          "hello world"
        )
      )
      .build()
    val allocator: BufferAllocator = new RootAllocator()
    val vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    vectorSchemaRoot.allocateNew()

    // set Tuple into the Vectors
    ArrowUtils.appendTexeraTuple(tuple, vectorSchemaRoot)

    // get the Tuple from the Vectors
    assert(ArrowUtils.getTexeraTuple(0, vectorSchemaRoot) == tuple)

  }

  it should "get Texera Tuple from Arrow Fields with null values correctly" in {

    val tuple = Tuple
      .builder(texeraSchema)
      .addSequentially(
        Array(
          Int.box(2),
          null,
          Boolean.box(true),
          Double.box(1.1),
          null,
          null
        )
      )
      .build()
    val allocator: BufferAllocator = new RootAllocator()
    val vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    vectorSchemaRoot.allocateNew()

    // set Tuple into the Vectors
    ArrowUtils.appendTexeraTuple(tuple, vectorSchemaRoot)

    // get the Tuple from the Vectors
    assert(ArrowUtils.getTexeraTuple(0, vectorSchemaRoot) == tuple)

  }

}
