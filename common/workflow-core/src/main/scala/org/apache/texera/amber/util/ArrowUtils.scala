/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.AttributeTypeException
import org.apache.texera.amber.core.tuple._
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.TimeUnit
import org.apache.arrow.vector.types.pojo.ArrowType.PrimitiveType
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType}
import org.apache.arrow.vector.{
  BigIntVector,
  BitVector,
  FieldVector,
  Float8Vector,
  IntVector,
  TimeStampVector,
  VarBinaryVector,
  VarCharVector,
  VectorSchemaRoot
}
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDateTime, ZoneId, ZoneOffset}
import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.language.implicitConversions

object ArrowUtils extends LazyLogging {
  private val TexeraTypeMetadataKey = "texera_type"

  // Create a single allocator for the entire utility
  private val allocator: BufferAllocator = new RootAllocator()

  implicit def bool2int(b: Boolean): Int = if (b) 1 else 0

  // Serialize a VectorSchemaRoot to Arrow IPC stream bytes. Does not close root.
  def serializeRoot(root: VectorSchemaRoot): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))
    try { writer.start(); writer.writeBatch(); writer.end() }
    finally writer.close()
    out.toByteArray
  }

  // Read one Arrow IPC batch, apply `f` to the live root, then close the reader.
  // `f` must copy anything it needs out (the root is freed on return).
  def deserializeRootFold[T](bytes: Array[Byte], allocator: BufferAllocator)(
      f: VectorSchemaRoot => T
  ): T = {
    val reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)
    try {
      reader.loadNextBatch()
      f(reader.getVectorSchemaRoot)
    } finally reader.close()
  }

  // Serialize a batch of tuples to Arrow IPC stream bytes (columnar wire format).
  def serializeTuples(schema: Schema, tuples: Array[Tuple]): Array[Byte] = {
    val root = VectorSchemaRoot.create(fromTexeraSchema(schema), allocator)
    try {
      var i = 0
      while (i < tuples.length) { setTexeraTuple(tuples(i), i, root); i += 1 }
      root.setRowCount(tuples.length)
      serializeRoot(root)
    } finally root.close()
  }

  // Inverse of serializeTuples: decode Arrow IPC stream bytes back to tuples.
  def deserializeTuples(bytes: Array[Byte]): Array[Tuple] = {
    val reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)
    try {
      val out = scala.collection.mutable.ArrayBuffer[Tuple]()
      val root = reader.getVectorSchemaRoot
      while (reader.loadNextBatch()) {
        val n = root.getRowCount
        var i = 0
        while (i < n) { out += getTexeraTuple(i, root); i += 1 }
      }
      out.toArray
    } finally reader.close()
  }

  // Copy the rows of `src` where keep(i) is true into a new independent root.
  def selectRows(
      src: VectorSchemaRoot,
      keep: Array[Boolean],
      allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val dest = VectorSchemaRoot.create(src.getSchema, allocator)
    val srcVecs = src.getFieldVectors
    val destVecs = dest.getFieldVectors
    var destIdx = 0
    var i = 0
    val n = src.getRowCount
    while (i < n) {
      if (keep(i)) {
        var c = 0
        while (c < srcVecs.size) {
          destVecs.get(c).copyFromSafe(i, destIdx, srcVecs.get(c)); c += 1
        }
        destIdx += 1
      }
      i += 1
    }
    dest.setRowCount(destIdx)
    dest
  }

  // Selection vector: the indices of `src`'s rows where keep(i) is true, in order.
  // Records which rows passed without copying any cell (unlike selectRows). A
  // consumer that only needs a count or an aggregate reads the indices and never
  // materializes; a consumer that needs contiguous rows calls `gather`.
  def selectionOf(keep: Array[Boolean], rowCount: Int): Array[Int] = {
    val sel = new Array[Int](rowCount)
    var n = 0
    var i = 0
    while (i < rowCount) { if (keep(i)) { sel(n) = i; n += 1 }; i += 1 }
    if (n == rowCount) sel else java.util.Arrays.copyOf(sel, n)
  }

  // Materialize the rows named by a selection vector into a new compacted root
  // (the deferred copy). Inverse endpoint of selectionOf for consumers that need
  // contiguous Arrow columns (e.g. shipping to the wire).
  def gather(
      src: VectorSchemaRoot,
      selection: Array[Int],
      allocator: BufferAllocator
  ): VectorSchemaRoot = {
    val dest = VectorSchemaRoot.create(src.getSchema, allocator)
    val srcVecs = src.getFieldVectors
    val destVecs = dest.getFieldVectors
    var di = 0
    while (di < selection.length) {
      val i = selection(di)
      var c = 0
      while (c < srcVecs.size) { destVecs.get(c).copyFromSafe(i, di, srcVecs.get(c)); c += 1 }
      di += 1
    }
    dest.setRowCount(selection.length)
    dest
  }

  // Resolve the column indices of `names` in an Arrow batch, by exact name.
  def projectionIndices(root: VectorSchemaRoot, names: Seq[String]): Array[Int] = {
    val fields = root.getSchema.getFields
    names.map { name =>
      var idx = -1
      var k = 0
      while (k < fields.size && idx < 0) { if (fields.get(k).getName == name) idx = k; k += 1 }
      if (idx < 0) throw new IllegalArgumentException(s"column '$name' not found in Arrow batch")
      idx
    }.toArray
  }

  // Read one cell as the Texera value for `attrType`, returning null on any
  // conversion failure (matches the row path's lenient decode).
  def getCell(vector: FieldVector, rowIndex: Int, attrType: AttributeType): AnyRef = {
    val raw = vector.getObject(rowIndex)
    try {
      attrType match {
        case AttributeType.TIMESTAMP => wallClockOf(raw, vector.getField.getType)
        case _                       => AttributeTypeUtils.parseField(raw, attrType).asInstanceOf[AnyRef]
      }
    } catch { case _: Exception => null }
  }

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
  ): Tuple = getTexeraTuple(rowIndex, vectorSchemaRoot, toTexeraSchema(vectorSchemaRoot.getSchema))

  /**
    * Reads a row like `getTexeraTuple(rowIndex, vectorSchemaRoot)`, given the root's schema
    * already converted by `toTexeraSchema`, so a caller reading many rows of one root converts
    * its schema once instead of once per row.
    *
    * @param schema `toTexeraSchema(vectorSchemaRoot.getSchema)`.
    */
  def getTexeraTuple(
      rowIndex: Int,
      vectorSchemaRoot: VectorSchemaRoot,
      schema: Schema
  ): Tuple = {
    val fieldVectors = vectorSchemaRoot.getFieldVectors
    val fieldValues = new Array[Any](fieldVectors.size())
    val vectors = fieldVectors.iterator()
    val attributes = schema.getAttributes.iterator
    var index = 0
    while (vectors.hasNext) {
      val fieldVector = vectors.next()
      val value: AnyRef = fieldVector.getObject(rowIndex)
      val fieldValue =
        try {
          // Use the attribute type from the schema (which includes metadata)
          // instead of deriving it from the Arrow type
          val attributeType = attributes.next().getType
          // A timestamp and an unsigned integer are the types whose field
          // says more than the schema does, so they are the only ones that
          // read the field: an unsigned column arrives as INTEGER or LONG.
          attributeType match {
            case AttributeType.TIMESTAMP => wallClockOf(value, fieldVector.getField.getType)
            case AttributeType.INTEGER | AttributeType.LONG =>
              AttributeTypeUtils.parseField(
                unsignedValueOf(value, fieldVector.getField.getType),
                attributeType
              )
            case _ => AttributeTypeUtils.parseField(value, attributeType)
          }
        } catch {
          case e: Exception =>
            logger.warn("Caught error during parsing Arrow value back to Texera value", e)
            null
        }
      fieldValues(index) = fieldValue
      index += 1
    }

    Tuple.builder(schema).addSequentially(fieldValues).build()
  }

  /** The number an unsigned column counts to, out of the storage it counts in.
    *
    * Arrow's unsigned vectors hand back the raw storage, signed: `UInt1Vector` a
    * Byte, `UInt4Vector` an Int, each reading the file's largest value as -1.
    * `UInt2Vector` is the exception and hands back a Character, which is already
    * the number but is no kind of integer to the parse below. Every other value
    * passes through untouched.
    *
    * Nothing wider is handled because nothing wider arrives: [[toAttributeType]]
    * refuses an unsigned 64-bit column, having no Texera type to hold it.
    */
  private def unsignedValueOf(value: AnyRef, arrowType: ArrowType): AnyRef =
    arrowType match {
      case int: ArrowType.Int if !int.getIsSigned =>
        value match {
          case byte: java.lang.Byte      => Int.box(byte & 0xff)
          case char: java.lang.Character => Int.box(char.toInt)
          case integer: Integer          => Long.box(integer.toLong & 0xffffffffL)
          case other                     => other
        }
      case _ => value
    }

  /** The wall clock a timestamp column holds, read the way its own field states.
    *
    * A Texera TIMESTAMP carries no zone, so the wall clock is the whole of what
    * crosses over, and the field says how to arrive at one: the zone its numbers
    * are counted in, and the unit they are counted in. A zoned vector hands back
    * that number bare, and letting `new Timestamp(number)` make the wall clock
    * would take it for milliseconds in the JVM's zone: one file would then say
    * different things on servers in different places, with nothing in the file to
    * account for the difference. A zoneless vector hands back a LocalDateTime
    * already, which is the wall clock itself.
    */
  private def wallClockOf(value: AnyRef, arrowType: ArrowType): Timestamp =
    (value, arrowType) match {
      case (null, _)               => null
      case (ldt: LocalDateTime, _) => Timestamp.valueOf(ldt)
      case (number: java.lang.Long, field: ArrowType.Timestamp) =>
        Timestamp.valueOf(
          LocalDateTime.ofInstant(instantOf(number, field.getUnit), zoneOf(field))
        )
      case (other, _) => AttributeTypeUtils.parseTimestamp(other)
    }

  /** The zone a timestamp field counts its numbers in. An unlabelled field counts
    * from the epoch with no zone in the picture, which is what UTC arithmetic is.
    */
  private def zoneOf(field: ArrowType.Timestamp): ZoneId =
    Option(field.getTimezone).map(ZoneId.of).getOrElse(ZoneOffset.UTC)

  /** The instant a zoned vector's bare number stands for. Arrow leaves those
    * unscaled, so the field's unit is what says how far from the epoch it reaches.
    */
  private def instantOf(number: Long, unit: TimeUnit): Instant =
    unit match {
      case TimeUnit.SECOND      => Instant.ofEpochSecond(number)
      case TimeUnit.MILLISECOND => Instant.ofEpochMilli(number)
      case TimeUnit.MICROSECOND => Instant.EPOCH.plus(number, ChronoUnit.MICROS)
      case TimeUnit.NANOSECOND  => Instant.EPOCH.plusNanos(number)
    }

  /** The number a field of this unit records an instant as, inverting [[instantOf]].
    * Microseconds are counted from the seconds, because ChronoUnit.MICROS goes
    * through nanoseconds and overflows past 2262, a year a Timestamp holds.
    */
  private def numberOf(instant: Instant, unit: TimeUnit): Long =
    unit match {
      case TimeUnit.SECOND      => instant.getEpochSecond
      case TimeUnit.MILLISECOND => instant.toEpochMilli
      case TimeUnit.MICROSECOND =>
        Math.addExact(Math.multiplyExact(instant.getEpochSecond, 1000000L), instant.getNano / 1000L)
      case TimeUnit.NANOSECOND => ChronoUnit.NANOS.between(Instant.EPOCH, instant)
    }

  /**
    * Converts an Arrow Schema into Texera Schema.
    * Checks field metadata to recover types that share an Arrow representation
    * (LARGE_BINARY and ANY both ride on Utf8).
    *
    * @param arrowSchema The Arrow Schema to be converted.
    * @return A Texera Schema.
    */
  def toTexeraSchema(arrowSchema: org.apache.arrow.vector.types.pojo.Schema): Schema =
    Schema(
      arrowSchema.getFields.asScala.iterator.map { field =>
        val taggedType = Option(field.getMetadata)
          .flatMap(m => Option(m.get(TexeraTypeMetadataKey)))
          .collect {
            case "LARGE_BINARY" => AttributeType.LARGE_BINARY
            case "ANY"          => AttributeType.ANY
          }

        val attributeType = taggedType.getOrElse(toAttributeType(field.getType))
        new Attribute(field.getName, attributeType)
      }.toList
    )

  /**
    * Converts an ArrowType into an AttributeType.
    *
    * @param srcType the ArrowType to be converted.
    * @throws org.apache.texera.amber.core.tuple.AttributeTypeUtils.AttributeTypeException if the type cannot be converted.
    * @return An AttributeType.
    */
  @throws[AttributeTypeException]
  def toAttributeType(srcType: ArrowType): AttributeType = {
    srcType match {
      // An unsigned column counts up where its storage counts down: the largest
      // unsigned 32-bit value is stored as -1, so read as its storage it would
      // arrive as -1 where the file means 4294967295. The next Texera integer up
      // holds it, and [[unsignedValueOf]] does the reading. Past 64 bits there is
      // no next one. The same widening covers the narrow widths, Texera having no
      // column shorter than a 32-bit integer.
      case int: ArrowType.Int =>
        (int.getBitWidth, int.getIsSigned) match {
          case (8 | 16 | 32, true) => AttributeType.INTEGER
          case (8 | 16, false)     => AttributeType.INTEGER
          case (64, true)          => AttributeType.LONG
          case (32, false)         => AttributeType.LONG
          case (64, false) =>
            throw new AttributeTypeUtils.AttributeTypeException(
              "Unsupported unsigned 64-bit Int, which is wider than any Texera column"
            )
          case (other, _) =>
            throw new AttributeTypeUtils.AttributeTypeException(
              s"Unsupported Int bit width: $other"
            )
        }
      case _: ArrowType.Bool =>
        AttributeType.BOOLEAN

      case _: ArrowType.FloatingPoint =>
        AttributeType.DOUBLE

      case _: ArrowType.Timestamp =>
        AttributeType.TIMESTAMP

      case _: ArrowType.Utf8 =>
        AttributeType.STRING

      case _: ArrowType.Binary =>
        AttributeType.BINARY

      case _ =>
        throw new AttributeTypeUtils.AttributeTypeException(
          "Unexpected value: " + srcType.getTypeID
        )
    }
  }

  def appendTexeraTuple(tuple: Tuple, vectorSchemaRoot: VectorSchemaRoot): Unit = {
    setTexeraTuple(tuple, vectorSchemaRoot.getRowCount, vectorSchemaRoot)
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
    val arrowFields = vectorSchemaRoot.getSchema.getFields

    for (i <- 0 until arrowFields.size()) {
      val vector: FieldVector = vectorSchemaRoot.getVector(i)
      val value = tuple.getField[AnyRef](i)
      val isNull = value == null
      arrowFields.get(i).getFieldType.getType match {
        case _: ArrowType.Int =>
          vector.getField.getFieldType.getType.asInstanceOf[ArrowType.Int].getBitWidth match {
            case 16 | 32 =>
              vector
                .asInstanceOf[IntVector]
                .setSafe(index, !isNull, if (isNull) 0 else value.asInstanceOf[Int])

            case 64 =>
              vector
                .asInstanceOf[BigIntVector]
                .setSafe(index, !isNull, if (isNull) 0 else value.asInstanceOf[Long])

            case other =>
              throw new AttributeTypeUtils.AttributeTypeException(
                s"Unsupported Int bit width: $other"
              )
          }

        case _: ArrowType.Bool =>
          vector
            .asInstanceOf[BitVector]
            .setSafe(index, !isNull, if (isNull) 0 else value.asInstanceOf[Boolean])

        case _: ArrowType.FloatingPoint =>
          vector
            .asInstanceOf[Float8Vector]
            .setSafe(index, !isNull, if (isNull) 0 else value.asInstanceOf[Double])

        // The wall clock written as the field's own zone and unit, so the number
        // and the label beside it agree. Going through the value's own epoch
        // would have read the wall clock in the JVM's zone instead, putting a
        // machine's setting into the file: the same table written in two places
        // would hold two different instants under one label. Inverts
        // [[wallClockOf]].
        case timestamp: ArrowType.Timestamp =>
          vector
            .asInstanceOf[TimeStampVector]
            .setSafe(
              index,
              !isNull,
              if (isNull) 0L
              else
                numberOf(
                  AttributeTypeUtils
                    .parseField(value, AttributeType.TIMESTAMP)
                    .asInstanceOf[Timestamp]
                    .toLocalDateTime
                    .atZone(zoneOf(timestamp))
                    .toInstant,
                  timestamp.getUnit
                )
            )

        case _: ArrowType.Utf8 =>
          if (isNull) vector.asInstanceOf[VarCharVector].setNull(index)
          else
            vector
              .asInstanceOf[VarCharVector]
              .setSafe(index, value.toString.getBytes(StandardCharsets.UTF_8))
        case _: ArrowType.Binary | _: ArrowType.LargeBinary =>
          if (isNull) vector.asInstanceOf[VarBinaryVector].setNull(index)
          else
            vector
              .asInstanceOf[VarBinaryVector]
              .setSafe(index, value.asInstanceOf[Array[Byte]])

      }
    }

    vectorSchemaRoot.setRowCount(vectorSchemaRoot.getRowCount + 1)
  }

  /**
    * Converts an Amber schema into Arrow schema.
    * Stores AttributeType in field metadata to preserve LARGE_BINARY and ANY,
    * which both collapse onto Utf8 in Arrow.
    *
    * @param schema The Texera Schema.
    * @return An Arrow Schema.
    */
  def fromTexeraSchema(schema: Schema): org.apache.arrow.vector.types.pojo.Schema = {
    val attributes = schema.getAttributes
    val arrowFields = new util.ArrayList[Field](attributes.size)
    attributes.foreach { attribute =>
      val metadataTag = attribute.getType match {
        case AttributeType.LARGE_BINARY => "LARGE_BINARY"
        case AttributeType.ANY          => "ANY"
        case _                          => null
      }
      val metadata = if (metadataTag != null) {
        val map = new util.HashMap[String, String]()
        map.put(TexeraTypeMetadataKey, metadataTag)
        map
      } else null

      arrowFields.add(
        new Field(
          attribute.getName,
          new FieldType(true, fromAttributeType(attribute.getType), null, metadata),
          null
        )
      )
    }

    new org.apache.arrow.vector.types.pojo.Schema(arrowFields)
  }

  /**
    * Converts an AttributeType into an ArrowType (PrimitiveType).
    *
    * @param srcType The AttributeType to be converted.
    * @throws org.apache.texera.amber.core.tuple.AttributeTypeUtils.AttributeTypeException if the type cannot be converted.
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

      // A wall clock with no zone, to the microsecond, which is what a Python
      // worker maps TIMESTAMP to and the finest a datetime holds. Labelled UTC
      // in milliseconds, a Python operator was handed an aware moment cut to
      // the millisecond, where a JVM operator reads the same row with no zone
      // and to the nanosecond.
      case AttributeType.TIMESTAMP =>
        new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)

      case AttributeType.BINARY =>
        new ArrowType.Binary

      case AttributeType.STRING | AttributeType.LARGE_BINARY | AttributeType.ANY =>
        ArrowType.Utf8.INSTANCE

      case _ =>
        throw new AttributeTypeUtils.AttributeTypeException("Unexpected value: " + srcType)
    }
  }
}
