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

package edu.uci.ics.amber.operator.sink

import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}

object ProgressiveUtils {

  // boolean attribute to indicate insertion / retraction
  // true  indicates insertion  (+)
  // false indicates retraction (-)
  val insertRetractFlagAttr = new Attribute("__internal_is_insertion", AttributeType.BOOLEAN)

  def addInsertionFlag(tuple: Tuple, outputSchema: Schema): Tuple = {
    assert(!tuple.getSchema.containsAttribute(insertRetractFlagAttr.getName))
    Tuple.builder(outputSchema).add(insertRetractFlagAttr, true).add(tuple).build()
  }

  def addRetractionFlag(tuple: Tuple, outputSchema: Schema): Tuple = {
    assert(!tuple.getSchema.containsAttribute(insertRetractFlagAttr.getName))
    Tuple.builder(outputSchema).add(insertRetractFlagAttr, false).add(tuple).build()
  }

  def isInsertion(tuple: Tuple): Boolean = {
    if (tuple.getSchema.containsAttribute(insertRetractFlagAttr.getName)) {
      tuple.getField[Boolean](insertRetractFlagAttr.getName)
    } else {
      true
    }
  }

  def getTupleFlagAndValue(
      tuple: Tuple
  ): (Boolean, Tuple) = {
    (
      isInsertion(tuple), {
        val originalSchema = tuple.getSchema
        val schema = originalSchema.getPartialSchema(
          originalSchema.getAttributeNames.filterNot(_ == insertRetractFlagAttr.getName)
        )
        Tuple.builder(schema).add(tuple, isStrictSchemaMatch = false).build()
      }
    )
  }

}
