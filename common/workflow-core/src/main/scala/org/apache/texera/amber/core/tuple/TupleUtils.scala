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
import org.apache.texera.amber.core.tuple.AttributeTypeUtils.{inferSchemaFromRows, parseField}
import org.apache.texera.amber.util.JSONUtils
import org.apache.texera.amber.util.JSONUtils.{JSONToMap, objectMapper}

import scala.collection.mutable.ArrayBuffer

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

  def json2tuple(json: String): Tuple = {
    var fieldNames = Set[String]()

    val allFields: ArrayBuffer[Map[String, String]] = ArrayBuffer()

    val root: JsonNode = objectMapper.readTree(json)
    if (root.isObject) {
      val fields: Map[String, String] = JSONToMap(root)
      fieldNames = fieldNames.++(fields.keySet)
      allFields += fields
    }

    val sortedFieldNames = fieldNames.toList

    val attributeTypes = inferSchemaFromRows(allFields.iterator.map(fields => {
      val result = ArrayBuffer[Object]()
      for (fieldName <- sortedFieldNames) {
        if (fields.contains(fieldName)) {
          result += fields(fieldName)
        } else {
          result += null
        }
      }
      result.toArray
    }))

    val schema = Schema(
      sortedFieldNames.indices
        .map(i => new Attribute(sortedFieldNames(i), attributeTypes(i)))
        .toList
    )

    try {
      val fields = scala.collection.mutable.ArrayBuffer.empty[Any]
      val data = JSONToMap(objectMapper.readTree(json))

      for (fieldName <- schema.getAttributeNames) {
        if (data.contains(fieldName)) {
          fields += parseField(data(fieldName), schema.getAttribute(fieldName).getType)
        } else {
          fields += null
        }
      }
      Tuple.builder(schema).addSequentially(fields.toArray).build()
    } catch {
      case e: Exception => throw e
    }
  }

}
