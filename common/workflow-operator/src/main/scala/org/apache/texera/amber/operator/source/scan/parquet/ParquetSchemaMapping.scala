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

package org.apache.texera.amber.operator.source.scan.parquet

import org.apache.parquet.schema.LogicalTypeAnnotation.{
  DateLogicalTypeAnnotation,
  DecimalLogicalTypeAnnotation,
  Float16LogicalTypeAnnotation,
  IntLogicalTypeAnnotation,
  IntervalLogicalTypeAnnotation,
  JsonLogicalTypeAnnotation,
  StringLogicalTypeAnnotation,
  TimeLogicalTypeAnnotation,
  TimestampLogicalTypeAnnotation
}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.{MessageType, Type}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}

import scala.jdk.CollectionConverters._

/**
  * What a Parquet file says its columns are, in Texera's terms.
  *
  * The file states its own types, so nothing is inferred from the values the way
  * a CSV forces. A column is read where Texera holds the value the file means by
  * it and the exported script reads that same value off the same bytes. The rest
  * are refused by name rather than silently dropped, stringified, or read as
  * something the file does not say.
  */
object ParquetSchemaMapping {

  /** Texera's reading of the file's own schema, in the file's column order. */
  def toTexeraSchema(messageType: MessageType): Schema =
    new Schema(
      messageType.getFields.asScala.toSeq
        .map(field => new Attribute(field.getName, typeOf(field))): _*
    )

  /** The Texera type a Parquet column is read as, or an error naming the column. */
  def typeOf(field: Type): AttributeType = {
    if (!field.isPrimitive) {
      refuse(
        field,
        s"a nested ${describe(field)}, which has no Texera column to be. " +
          "Flatten it before reading the file."
      )
    }
    // A repeated column holds a list per row, and a Texera cell holds one value.
    // Keeping the first of them would drop the rest in silence, where pandas
    // hands the script the whole list.
    if (field.isRepetition(Repetition.REPEATED)) {
      refuse(
        field,
        "repeated, so it holds a list per row rather than one value. " +
          "Flatten it before reading the file."
      )
    }
    val primitive = field.asPrimitiveType()
    primitive.getLogicalTypeAnnotation match {
      // A DECIMAL is an integer that a scale moves the point in, and Texera has no
      // column that holds one exactly. Read as the number it stands for: the column
      // it is stored in holds 1234 where the file means 12.34, and a reader that
      // took the storage for the value would be off by a factor of the scale.
      case _: DecimalLogicalTypeAnnotation => AttributeType.DOUBLE
      case _: DateLogicalTypeAnnotation | _: TimestampLogicalTypeAnnotation =>
        AttributeType.TIMESTAMP
      // JSON is text that carries a grammar. The grammar is not Texera's to keep,
      // but the text is, and pandas reads that column as text too. ENUM and BSON
      // are bytes on both sides, so they stay with their storage below.
      case _: StringLogicalTypeAnnotation | _: JsonLogicalTypeAnnotation => AttributeType.STRING
      // An unsigned column counts up where its storage counts down: the largest
      // unsigned 32-bit value is stored as -1, and read as its storage it would
      // arrive as -1 where pandas reads 4294967295. The next Texera integer up
      // holds it. Past 64 bits there is no next one.
      case annotation: IntLogicalTypeAnnotation if !annotation.isSigned =>
        if (annotation.getBitWidth == 64) {
          refuse(field, "an unsigned 64-bit integer, which is wider than any Texera column.")
        }
        AttributeType.LONG
      // A time of day is not a moment, an interval is three counts at once, and a
      // 16-bit float is a number Texera would hand back as its two raw bytes.
      // pandas reads each of them as the value the file means, so a column read as
      // its storage here would part the engine from the script.
      case _: TimeLogicalTypeAnnotation =>
        refuse(field, "a time of day, which has no Texera column to be.")
      case _: IntervalLogicalTypeAnnotation =>
        refuse(field, "an interval, which has no Texera column to be.")
      case _: Float16LogicalTypeAnnotation =>
        refuse(field, "a 16-bit float, which has no Texera column to be.")
      case _ => storageOf(primitive.getPrimitiveTypeName)
    }
  }

  /** The Texera type of a column the file says no more about than its storage. */
  private def storageOf(storage: PrimitiveTypeName): AttributeType =
    storage match {
      case PrimitiveTypeName.BOOLEAN                          => AttributeType.BOOLEAN
      case PrimitiveTypeName.INT32                            => AttributeType.INTEGER
      case PrimitiveTypeName.INT64                            => AttributeType.LONG
      case PrimitiveTypeName.FLOAT | PrimitiveTypeName.DOUBLE => AttributeType.DOUBLE
      // The timestamp the older writers wrote, before there was an annotation for
      // one: a Julian day and the nanoseconds into it, in twelve bytes.
      case PrimitiveTypeName.INT96 => AttributeType.TIMESTAMP
      case PrimitiveTypeName.BINARY | PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
        AttributeType.BINARY
    }

  private def refuse(field: Type, what: String): Nothing =
    throw new UnsupportedOperationException(s"Parquet column '${field.getName}' is $what")

  private def describe(field: Type): String =
    Option(field.getLogicalTypeAnnotation).map(_.toString).getOrElse("group")
}
