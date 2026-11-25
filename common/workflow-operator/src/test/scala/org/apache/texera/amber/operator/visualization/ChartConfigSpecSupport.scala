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

package org.apache.texera.amber.operator.visualization

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.Assertions

import javax.validation.constraints.NotNull

/** Shared assertion helpers for chart config serialization, annotation, and bounds specs. */
private[visualization] trait ChartConfigSpecSupport extends Assertions {

  protected def checkRoundTrip[T](value: T, clazz: Class[T]): T =
    objectMapper.readValue(objectMapper.writeValueAsString(value), clazz)

  protected def assertAttrJsonPropertyRequired(clazz: Class[_]): Unit = {
    val jp = clazz.getDeclaredField("attributeName").getAnnotation(classOf[JsonProperty])
    assert(jp != null && jp.required, "attributeName must carry @JsonProperty(required = true)")
  }

  protected def assertAttrAutofill(clazz: Class[_], msg: String): Unit = {
    val ann = clazz.getDeclaredField("attributeName").getAnnotation(classOf[AutofillAttributeName])
    assert(ann != null, msg)
  }

  protected def assertAttrNotNull(clazz: Class[_]): Unit = {
    val ann = clazz.getDeclaredField("attributeName").getAnnotation(classOf[NotNull])
    assert(ann != null && ann.message == "Attribute Name cannot be empty")
  }

  protected def assertStepWireKeys[T](step: T, expectedStart: Double, expectedEnd: Double): Unit = {
    val tree = objectMapper.readTree(objectMapper.writeValueAsString(step))
    assert(
      tree
        .has("start") && tree.get("start").isNumber && tree.get("start").asDouble() == expectedStart
    )
    assert(tree.has("end") && tree.get("end").isNumber && tree.get("end").asDouble() == expectedEnd)
  }

  protected def assertBoundsPair[T](
      clazz: Class[T],
      json: String,
      boundsOf: T => (Option[Double], Option[Double]),
      expectedStart: Double,
      expectedEnd: Double
  ): Unit = {
    val (s, e) = boundsOf(objectMapper.readValue(json, clazz))
    assert(s.contains(expectedStart))
    assert(e.contains(expectedEnd))
  }

  protected def assertUnsetBounds[T](clazz: Class[T], startOf: T => Option[Double]): Unit = {
    assert(startOf(objectMapper.readValue("""{}""", clazz)).isEmpty)
    assert(startOf(objectMapper.readValue("""{"start":null}""", clazz)).isEmpty)
    assert(startOf(objectMapper.readValue("""{"start":""}""", clazz)).isEmpty)
  }

  protected def assertDoubleBoundsConversion[T](
      clazz: Class[T],
      startOf: T => Option[Double]
  ): Unit = {
    assert(startOf(objectMapper.readValue("""{"start":"1.5"}""", clazz)).map(_ * 2).contains(3.0))
  }
}
