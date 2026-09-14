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

import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.{
  DateLogicalTypeAnnotation,
  DecimalLogicalTypeAnnotation,
  StringLogicalTypeAnnotation,
  TimestampLogicalTypeAnnotation
}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.{MessageType, PrimitiveType, Type}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}

import scala.jdk.CollectionConverters._

/**
  * What a Parquet file says its columns are, in Texera's terms.
  *
  * The file states its own types, so nothing is inferred from the values the way
  * a CSV forces. Only the flat primitives map: a column that is a group, a list
  * or a map has no Texera column to be, and is refused by name rather than
  * silently dropped or stringified.
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
      throw new UnsupportedOperationException(
        s"Parquet column '${field.getName}' is a nested ${describe(field)}, which has no Texera " +
          "column to be. Flatten it before reading the file."
      )
    }
    val primitive = field.asPrimitiveType()
    // A DECIMAL is an integer that a scale moves the point in, and Texera has no
    // column that holds one exactly. Read as the number it stands for: the column
    // it is stored in holds 1234 where the file means 12.34, and a reader that
    // took the storage for the value would be off by a factor of the scale.
    if (primitive.getLogicalTypeAnnotation.isInstanceOf[DecimalLogicalTypeAnnotation]) {
      return AttributeType.DOUBLE
    }
    primitive.getPrimitiveTypeName match {
      case PrimitiveTypeName.BOOLEAN                          => AttributeType.BOOLEAN
      case PrimitiveTypeName.FLOAT | PrimitiveTypeName.DOUBLE => AttributeType.DOUBLE
      case PrimitiveTypeName.INT32 =>
        annotated[DateLogicalTypeAnnotation](
          primitive,
          AttributeType.TIMESTAMP,
          AttributeType.INTEGER
        )
      case PrimitiveTypeName.INT64 =>
        annotated[TimestampLogicalTypeAnnotation](
          primitive,
          AttributeType.TIMESTAMP,
          AttributeType.LONG
        )
      case PrimitiveTypeName.BINARY =>
        annotated[StringLogicalTypeAnnotation](
          primitive,
          AttributeType.STRING,
          AttributeType.BINARY
        )
      case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY | PrimitiveTypeName.INT96 => AttributeType.BINARY
    }
  }

  /** `whenAnnotated` if the column carries annotation `A`, `otherwise` if it does not. */
  private def annotated[A <: LogicalTypeAnnotation](
      primitive: PrimitiveType,
      whenAnnotated: AttributeType,
      otherwise: AttributeType
  )(implicit tag: scala.reflect.ClassTag[A]): AttributeType =
    if (tag.runtimeClass.isInstance(primitive.getLogicalTypeAnnotation)) whenAnnotated
    else otherwise

  private def describe(field: Type): String =
    Option(field.getLogicalTypeAnnotation).map(_.toString).getOrElse("group")
}
