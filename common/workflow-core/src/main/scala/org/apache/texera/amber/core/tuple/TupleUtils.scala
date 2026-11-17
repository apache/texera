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

package org.apache.texera.amber.core.tuple

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.util.JSONUtils

import java.lang.Byte.{SIZE => BitsPerByte}

object TupleUtils {

  def tuple2json(schema: Schema, fieldVals: Array[Any]): ObjectNode = {
    val objectNode = JSONUtils.objectMapper.createObjectNode()
    schema.getAttributeNames.foreach { attrName =>
      val valueNode =
        JSONUtils.objectMapper.convertValue(fieldVals(schema.getIndex(attrName)), classOf[JsonNode])
      objectNode.set[ObjectNode](attrName, valueNode)
    }
    objectNode
  }

  private val binaryPreviewLeadingBits: Int = 10
  private val binaryPreviewTrailingBits: Int = 3

  private def bytesToBinaryString(bytes: Array[Byte]): String =
    bytes
      .map(b =>
        String.format(s"%${BitsPerByte}s", Integer.toBinaryString(b & 0xff)).replace(' ', '0')
      )
      .mkString("")

  /**
    * Converts a collection of Tuples to a list of JSON ObjectNodes.
    *
    * This function takes a collection of Tuples and converts each tuple into a JSON ObjectNode.
    * For binary data, it formats the bytes into a readable hex string representation with length info.
    * For string values longer than maxStringLength (100), it truncates them.
    * NULL values are converted to the string "NULL".
    *
    * @param tuples The collection of Tuples to convert
    * @param isVisualization Whether this is for visualization rendering (affects string truncation)
    * @return A List of ObjectNodes containing the JSON representation of the tuples
    */
  def convertTuplesToJson(
      tuples: Iterable[Tuple],
      isVisualization: Boolean = false
  ): List[ObjectNode] = {
    val maxStringLength = 100

    tuples.map { tuple =>
      val processedFields = tuple.schema.getAttributes.zipWithIndex
        .map {
          case (attr, idx) =>
            val fieldValue = tuple.getField[AnyRef](idx)

            Option(fieldValue) match {
              case None => "NULL"
              case Some(value) =>
                attr.getType match {
                  case AttributeType.BINARY =>
                    value match {
                      case byteArray: Array[Byte] =>
                        val totalSize = byteArray.length
                        val sizeFormatted = f"$totalSize%,d"
                        val totalBits = totalSize * BitsPerByte
                        val preview =
                          if (totalBits <= binaryPreviewLeadingBits + binaryPreviewTrailingBits)
                            bytesToBinaryString(byteArray)
                          else {
                            val leadingBytesNeeded =
                              math.ceil(binaryPreviewLeadingBits.toDouble / BitsPerByte).toInt
                            val trailingBytesNeeded =
                              math.ceil(binaryPreviewTrailingBits.toDouble / BitsPerByte).toInt
                            val leading = bytesToBinaryString(byteArray.take(leadingBytesNeeded))
                              .take(binaryPreviewLeadingBits)
                            val trailing = bytesToBinaryString(
                              byteArray.takeRight(trailingBytesNeeded)
                            ).takeRight(binaryPreviewTrailingBits)
                            s"$leading...$trailing"
                          }
                        s"<binary $preview, size = $sizeFormatted bytes>"

                      case _ =>
                        throw new RuntimeException(
                          s"Expected byte array for binary type field, but got: ${value.getClass.getName}"
                        )
                    }
                  case AttributeType.STRING =>
                    val stringValue = value.asInstanceOf[String]
                    if (stringValue.length > maxStringLength && !isVisualization)
                      stringValue.take(maxStringLength) + "..."
                    else
                      stringValue
                  case _ => value
                }
            }
        }
        .toArray[Any]

      TupleUtils.tuple2json(tuple.schema, processedFields)
    }.toList
  }

}
