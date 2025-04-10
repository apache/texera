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

  val texeraSchema: Schema = Schema()
    .add("test-1", AttributeType.INTEGER)
    .add("test-2", AttributeType.LONG)
    .add("test-3", AttributeType.BOOLEAN)
    .add("test-4", AttributeType.DOUBLE)
    .add("test-5", AttributeType.TIMESTAMP)
    .add("test-6", AttributeType.STRING)

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
    assert(ArrowUtils.fromAttributeTypeToPrimitive(AttributeType.INTEGER) == signedInt)
    assert(ArrowUtils.fromAttributeTypeToPrimitive(AttributeType.LONG) == signedLongInt)
    assert(ArrowUtils.fromAttributeTypeToPrimitive(AttributeType.BOOLEAN) == boolean)
    assert(ArrowUtils.fromAttributeTypeToPrimitive(AttributeType.DOUBLE) == double)
    assert(ArrowUtils.fromAttributeTypeToPrimitive(AttributeType.TIMESTAMP) == timestamp)
    assert(ArrowUtils.fromAttributeTypeToPrimitive(AttributeType.STRING) == string)

    // a special case that's not symmetric, AttributeType.ANY will be converted to ArrowType.Utf8
    // but not the other way around.
    assert(ArrowUtils.fromAttributeTypeToPrimitive(AttributeType.ANY) == string)

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

  it should "handle binary data with LargeListVector correctly" in {
    val binarySchema = Schema()
      .add("id", AttributeType.INTEGER)
      .add("binary-data", AttributeType.BINARY)
      .add("name", AttributeType.STRING)

    // Create sample binary data using ByteBuffers
    val bytes1 = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val bytes2 = "world".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val binaryList = List(
      java.nio.ByteBuffer.wrap(bytes1),
      java.nio.ByteBuffer.wrap(bytes2)
    )

    // Create a tuple with binary data
    val tuple = Tuple
      .builder(binarySchema)
      .addSequentially(
        Array(
          Int.box(1),
          binaryList,
          "test binary"
        )
      )
      .build()

    val allocator: BufferAllocator = new RootAllocator()
    val arrowSchema = ArrowUtils.fromTexeraSchema(binarySchema)
    val vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    vectorSchemaRoot.allocateNew()

    // Set tuple into Arrow vectors
    ArrowUtils.appendTexeraTuple(tuple, vectorSchemaRoot)

    // Get the tuple back from Arrow vectors
    val retrievedTuple = ArrowUtils.getTexeraTuple(0, vectorSchemaRoot)

    // Verify the retrieved tuple matches the original
    assert(retrievedTuple.getField[Int](0) == 1)
    assert(retrievedTuple.getField[String](2) == "test binary")

    // Verify binary data
    val retrievedBinaryList = retrievedTuple.getField[List[java.nio.ByteBuffer]](1)
    assert(retrievedBinaryList.size == 2)

    // Convert ByteBuffers to strings for comparison
    val retrievedString1 = new String(
      retrievedBinaryList(0).array(),
      java.nio.charset.StandardCharsets.UTF_8
    )
    val retrievedString2 = new String(
      retrievedBinaryList(1).array(),
      java.nio.charset.StandardCharsets.UTF_8
    )

    assert(retrievedString1 == "hello")
    assert(retrievedString2 == "world")
  }

  it should "handle empty binary lists correctly" in {
    val binarySchema = Schema()
      .add("id", AttributeType.INTEGER)
      .add("binary-data", AttributeType.BINARY)

    // Create a tuple with empty binary list
    val tuple = Tuple
      .builder(binarySchema)
      .addSequentially(
        Array(
          Int.box(1),
          List[java.nio.ByteBuffer]()
        )
      )
      .build()

    val allocator: BufferAllocator = new RootAllocator()
    val arrowSchema = ArrowUtils.fromTexeraSchema(binarySchema)
    val vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    vectorSchemaRoot.allocateNew()

    // Set tuple into Arrow vectors
    ArrowUtils.appendTexeraTuple(tuple, vectorSchemaRoot)

    // Get the tuple back from Arrow vectors
    val retrievedTuple = ArrowUtils.getTexeraTuple(0, vectorSchemaRoot)

    // Verify the retrieved tuple matches the original
    assert(retrievedTuple.getField[Int](0) == 1)

    // Verify empty binary list
    val retrievedBinaryList = retrievedTuple.getField[List[java.nio.ByteBuffer]](1)
    assert(retrievedBinaryList.isEmpty)
  }

  it should "handle null binary data correctly" in {
    val binarySchema = Schema()
      .add("id", AttributeType.INTEGER)
      .add("binary-data", AttributeType.BINARY)

    // Create a tuple with null binary data
    val tuple = Tuple
      .builder(binarySchema)
      .addSequentially(
        Array(
          Int.box(1),
          null
        )
      )
      .build()

    val allocator: BufferAllocator = new RootAllocator()
    val arrowSchema = ArrowUtils.fromTexeraSchema(binarySchema)
    val vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    vectorSchemaRoot.allocateNew()

    // Set tuple into Arrow vectors
    ArrowUtils.appendTexeraTuple(tuple, vectorSchemaRoot)

    // Get the tuple back from Arrow vectors
    val retrievedTuple = ArrowUtils.getTexeraTuple(0, vectorSchemaRoot)

    // Verify the retrieved tuple matches the original
    assert(retrievedTuple.getField[Int](0) == 1)
    assert(retrievedTuple.getField[List[java.nio.ByteBuffer]](1) == null)
  }

  it should "handle binary data with empty ByteBuffers correctly" in {
    val binarySchema = Schema()
      .add("binary-data", AttributeType.BINARY)

    // Create empty ByteBuffer
    val emptyBuffer = java.nio.ByteBuffer.allocate(0)
    val binaryList = List(emptyBuffer)

    // Create a tuple with empty ByteBuffer
    val tuple = Tuple
      .builder(binarySchema)
      .addSequentially(
        Array(
          binaryList
        )
      )
      .build()

    val allocator: BufferAllocator = new RootAllocator()
    val arrowSchema = ArrowUtils.fromTexeraSchema(binarySchema)
    val vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    vectorSchemaRoot.allocateNew()

    // Set tuple into Arrow vectors
    ArrowUtils.appendTexeraTuple(tuple, vectorSchemaRoot)

    // Get the tuple back from Arrow vectors
    val retrievedTuple = ArrowUtils.getTexeraTuple(0, vectorSchemaRoot)

    // Verify binary data with empty ByteBuffer
    val retrievedBinaryList = retrievedTuple.getField[List[java.nio.ByteBuffer]](0)
    assert(retrievedBinaryList.size == 1)
    assert(retrievedBinaryList(0).capacity() == 0)
  }

  it should "handle multiple binary buffers of different sizes correctly" in {
    val binarySchema = Schema()
      .add("binary-data", AttributeType.BINARY)

    // Create sample binary data with varying sizes
    val bytes1 = Array.fill[Byte](1024)(1) // 1KB of data filled with 1s
    val bytes2 = Array.fill[Byte](10)(2) // 10 bytes filled with 2s
    val bytes3 = Array.fill[Byte](5000)(3) // 5KB of data filled with 3s

    val binaryList = List(
      java.nio.ByteBuffer.wrap(bytes1),
      java.nio.ByteBuffer.wrap(bytes2),
      java.nio.ByteBuffer.wrap(bytes3)
    )

    // Create a tuple with binary data
    val tuple = Tuple
      .builder(binarySchema)
      .addSequentially(
        Array(
          binaryList
        )
      )
      .build()

    val allocator: BufferAllocator = new RootAllocator()
    val arrowSchema = ArrowUtils.fromTexeraSchema(binarySchema)
    val vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    vectorSchemaRoot.allocateNew()

    // Set tuple into Arrow vectors
    ArrowUtils.appendTexeraTuple(tuple, vectorSchemaRoot)

    // Get the tuple back from Arrow vectors
    val retrievedTuple = ArrowUtils.getTexeraTuple(0, vectorSchemaRoot)

    // Verify binary data
    val retrievedBinaryList = retrievedTuple.getField[List[java.nio.ByteBuffer]](0)
    assert(retrievedBinaryList.size == 3)

    // Verify contents of each buffer
    val retrieved1 = retrievedBinaryList(0)
    val retrieved2 = retrievedBinaryList(1)
    val retrieved3 = retrievedBinaryList(2)

    assert(retrieved1.remaining() == 1024)
    assert(retrieved2.remaining() == 10)
    assert(retrieved3.remaining() == 5000)

    // Check content of first byte in each buffer
    assert(retrieved1.get(0) == 1)
    assert(retrieved2.get(0) == 2)
    assert(retrieved3.get(0) == 3)
  }

  it should "handle a list with mixed ByteBuffers and nulls" in {
    val binarySchema = Schema()
      .add("binary-data", AttributeType.BINARY)

    // Create sample binary data with one null element
    val bytes1 = "first".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val bytes2 = "second".getBytes(java.nio.charset.StandardCharsets.UTF_8)

    // Use null as the second element (note: this requires modifications to ArrowUtils to support)
    val binaryList = List(
      java.nio.ByteBuffer.wrap(bytes1),
      null,
      java.nio.ByteBuffer.wrap(bytes2)
    ).asInstanceOf[List[java.nio.ByteBuffer]] // Cast needed to compile, though it contains null

    val tuple = Tuple
      .builder(binarySchema)
      .addSequentially(
        Array(
          binaryList
        )
      )
      .build()

    val allocator: BufferAllocator = new RootAllocator()
    val arrowSchema = ArrowUtils.fromTexeraSchema(binarySchema)
    val vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    vectorSchemaRoot.allocateNew()

    // This might throw an exception with the current implementation
    // If the implementation is enhanced to support nulls in the list, this test can verify it
    ArrowUtils.appendTexeraTuple(tuple, vectorSchemaRoot)

    val retrievedTuple = ArrowUtils.getTexeraTuple(0, vectorSchemaRoot)
    val retrievedBinaryList = retrievedTuple.getField[List[java.nio.ByteBuffer]](0)

    assert(retrievedBinaryList.size == 3)

    // First buffer should contain "first"
    val retrievedString1 = new String(
      retrievedBinaryList(0).array(),
      java.nio.charset.StandardCharsets.UTF_8
    )
    assert(retrievedString1 == "first")

    // Second buffer should be empty (representing null)
    assert(retrievedBinaryList(1).remaining() == 0)

    // Third buffer should contain "second"
    val retrievedString3 = new String(
      retrievedBinaryList(2).array(),
      java.nio.charset.StandardCharsets.UTF_8
    )
    assert(retrievedString3 == "second")
  }

  it should "correctly handle binary data across multiple rows" in {
    val binarySchema = Schema()
      .add("id", AttributeType.INTEGER)
      .add("binary-data", AttributeType.BINARY)

    // Create sample binary data for two rows
    val bytes1 = "row1data".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val bytes2 = "row2data".getBytes(java.nio.charset.StandardCharsets.UTF_8)

    val tuple1 = Tuple
      .builder(binarySchema)
      .addSequentially(
        Array(
          Int.box(1),
          List(java.nio.ByteBuffer.wrap(bytes1))
        )
      )
      .build()

    val tuple2 = Tuple
      .builder(binarySchema)
      .addSequentially(
        Array(
          Int.box(2),
          List(java.nio.ByteBuffer.wrap(bytes2))
        )
      )
      .build()

    val allocator: BufferAllocator = new RootAllocator()
    val arrowSchema = ArrowUtils.fromTexeraSchema(binarySchema)
    val vectorSchemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
    vectorSchemaRoot.allocateNew()

    // Add two rows
    ArrowUtils.appendTexeraTuple(tuple1, vectorSchemaRoot)
    ArrowUtils.appendTexeraTuple(tuple2, vectorSchemaRoot)

    // Verify both rows
    val retrievedTuple1 = ArrowUtils.getTexeraTuple(0, vectorSchemaRoot)
    val retrievedTuple2 = ArrowUtils.getTexeraTuple(1, vectorSchemaRoot)

    // Check IDs
    assert(retrievedTuple1.getField[Int](0) == 1)
    assert(retrievedTuple2.getField[Int](0) == 2)

    // Check binary data
    val retrievedList1 = retrievedTuple1.getField[List[java.nio.ByteBuffer]](1)
    val retrievedList2 = retrievedTuple2.getField[List[java.nio.ByteBuffer]](1)

    val retrievedString1 = new String(
      retrievedList1(0).array(),
      java.nio.charset.StandardCharsets.UTF_8
    )
    val retrievedString2 = new String(
      retrievedList2(0).array(),
      java.nio.charset.StandardCharsets.UTF_8
    )

    assert(retrievedString1 == "row1data")
    assert(retrievedString2 == "row2data")
  }

}
