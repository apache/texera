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

package org.apache.texera.amber.operator.source.scan.smart

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.{
  DateLogicalTypeAnnotation,
  StringLogicalTypeAnnotation,
  TimestampLogicalTypeAnnotation
}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{MessageType, PrimitiveType, Type}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}

import java.io.File

object ParquetUtils {

  /** Map a Parquet `MessageType` to a Texera Schema. Skips non-primitive (nested) fields. */
  def toTexeraSchema(messageType: MessageType): Schema = {
    val attrs = scala.collection.mutable.ListBuffer.empty[Attribute]
    val fieldCount = messageType.getFieldCount
    var i = 0
    while (i < fieldCount) {
      val field: Type = messageType.getType(i)
      if (field.isPrimitive) {
        attrs += new Attribute(field.getName, toAttributeType(field.asPrimitiveType()))
      }
      i += 1
    }
    Schema(attrs.toList)
  }

  def toAttributeType(primitive: PrimitiveType): AttributeType = {
    val logical = primitive.getLogicalTypeAnnotation
    primitive.getPrimitiveTypeName match {
      case PrimitiveTypeName.BOOLEAN => AttributeType.BOOLEAN
      case PrimitiveTypeName.INT32 =>
        logical match {
          case _: DateLogicalTypeAnnotation => AttributeType.TIMESTAMP
          case _                            => AttributeType.INTEGER
        }
      case PrimitiveTypeName.INT64 =>
        logical match {
          case _: TimestampLogicalTypeAnnotation => AttributeType.TIMESTAMP
          case _                                 => AttributeType.LONG
        }
      case PrimitiveTypeName.FLOAT | PrimitiveTypeName.DOUBLE => AttributeType.DOUBLE
      case PrimitiveTypeName.INT96                            => AttributeType.TIMESTAMP
      case PrimitiveTypeName.BINARY =>
        logical match {
          case _: StringLogicalTypeAnnotation => AttributeType.STRING
          case _ if isStringLike(logical)     => AttributeType.STRING
          case _                              => AttributeType.BINARY
        }
      case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => AttributeType.BINARY
    }
  }

  private def isStringLike(logical: LogicalTypeAnnotation): Boolean = {
    if (logical == null) return false
    // EnumLogicalTypeAnnotation / JsonLogicalTypeAnnotation also serialize as text.
    val name = logical.toString.toLowerCase
    name.contains("string") || name.contains("enum") || name.contains("json")
  }

  /** Opens a `ParquetFileReader` on a local file. */
  def openReader(file: File): ParquetFileReader = {
    val conf = newConfiguration()
    val inputFile = HadoopInputFile.fromPath(new Path(file.toURI), conf)
    ParquetFileReader.open(inputFile)
  }

  /**
    * Read the file into a lazy iterator of `Group` records.
    * Caller is responsible for closing the returned reader via [[ParquetReadHandle.close]].
    */
  def openRecords(file: File): ParquetReadHandle = {
    val conf = newConfiguration()
    val inputFile = HadoopInputFile.fromPath(new Path(file.toURI), conf)
    val reader = ParquetFileReader.open(inputFile)
    val schema = reader.getFooter.getFileMetaData.getSchema
    val converter = new GroupRecordConverter(schema)
    val columnIO = new ColumnIOFactory().getColumnIO(schema)
    val iterator = new Iterator[Group] {
      private var currentPages = reader.readNextRowGroup()
      private var recordReader =
        if (currentPages != null) columnIO.getRecordReader(currentPages, converter) else null
      private var remaining: Long = if (currentPages != null) currentPages.getRowCount else 0L

      override def hasNext: Boolean = {
        if (remaining > 0) return true
        // Advance to next row group.
        var nextPages = reader.readNextRowGroup()
        while (nextPages != null && nextPages.getRowCount == 0) nextPages = reader.readNextRowGroup()
        if (nextPages == null) false
        else {
          currentPages = nextPages
          recordReader = columnIO.getRecordReader(nextPages, converter)
          remaining = nextPages.getRowCount
          true
        }
      }

      override def next(): Group = {
        if (!hasNext) throw new NoSuchElementException
        remaining -= 1
        recordReader.read().asInstanceOf[Group]
      }
    }
    ParquetReadHandle(schema, iterator, () => reader.close())
  }

  /** Read a primitive field at position `index` of a Parquet `Group`, honoring schema. */
  def readField(group: Group, index: Int, schema: MessageType): Any = {
    if (group.getFieldRepetitionCount(index) == 0) return null
    val field = schema.getType(index)
    if (!field.isPrimitive) return null
    val primitive = field.asPrimitiveType()
    primitive.getPrimitiveTypeName match {
      case PrimitiveTypeName.BOOLEAN => group.getBoolean(index, 0)
      case PrimitiveTypeName.INT32 =>
        primitive.getLogicalTypeAnnotation match {
          case _: DateLogicalTypeAnnotation =>
            // Date stored as days since epoch.
            val days = group.getInteger(index, 0).toLong
            new java.sql.Timestamp(days * 86400000L)
          case _ => Int.box(group.getInteger(index, 0))
        }
      case PrimitiveTypeName.INT64 =>
        primitive.getLogicalTypeAnnotation match {
          case ts: TimestampLogicalTypeAnnotation =>
            val raw = group.getLong(index, 0)
            val millis = ts.getUnit match {
              case LogicalTypeAnnotation.TimeUnit.MILLIS => raw
              case LogicalTypeAnnotation.TimeUnit.MICROS => raw / 1000L
              case LogicalTypeAnnotation.TimeUnit.NANOS  => raw / 1000000L
            }
            new java.sql.Timestamp(millis)
          case _ => Long.box(group.getLong(index, 0))
        }
      case PrimitiveTypeName.FLOAT  => Double.box(group.getFloat(index, 0).toDouble)
      case PrimitiveTypeName.DOUBLE => Double.box(group.getDouble(index, 0))
      case PrimitiveTypeName.INT96 =>
        // INT96 → 96-bit timestamp; convert via Parquet's NanoTime helper.
        val binary = group.getInt96(index, 0)
        int96ToTimestamp(binary.getBytes)
      case PrimitiveTypeName.BINARY =>
        val binary = group.getBinary(index, 0)
        primitive.getLogicalTypeAnnotation match {
          case _: StringLogicalTypeAnnotation              => binary.toStringUsingUTF8
          case logical if isStringLike(logical)            => binary.toStringUsingUTF8
          case _                                           => binary.getBytes
        }
      case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => group.getBinary(index, 0).getBytes
    }
  }

  private def int96ToTimestamp(bytes: Array[Byte]): java.sql.Timestamp = {
    // INT96: 8 bytes little-endian nanoseconds of day, then 4 bytes little-endian Julian day.
    var nanos: Long = 0L
    for (i <- 0 until 8) nanos |= (bytes(i).toLong & 0xff) << (8 * i)
    var julian: Int = 0
    for (i <- 0 until 4) julian |= (bytes(8 + i).toInt & 0xff) << (8 * i)
    val daysFromEpoch = julian - 2440588 // Julian day 2440588 = 1970-01-01
    val millis = daysFromEpoch.toLong * 86400000L + nanos / 1000000L
    new java.sql.Timestamp(millis)
  }

  private def newConfiguration(): Configuration = {
    val conf = new Configuration(false)
    // Reduce noisy default classpath probing — we only ever look at local files.
    conf.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    conf
  }

  case class ParquetReadHandle(
      schema: MessageType,
      records: Iterator[Group],
      closer: () => Unit
  ) {
    def close(): Unit = closer()
  }
}
