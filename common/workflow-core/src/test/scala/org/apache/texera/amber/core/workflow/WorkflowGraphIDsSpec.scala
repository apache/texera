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

package org.apache.texera.amber.core.workflow

import com.fasterxml.jackson.databind.JsonNode
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import scala.jdk.CollectionConverters._

class WorkflowGraphIDsSpec extends AnyFlatSpec {
  private val matrix = objectMapper.readTree(getClass.getResource("/workflow-graph-ids.json"))
  private val collections =
    matrix.path("collections").fields().asScala.map(e => e.getKey -> e.getValue.asText()).toList

  private def check(name: String, content: JsonNode, expected: Option[String]): Unit =
    "Workflow graph identities" should name in {
      val before = content.toString
      expected match {
        case Some(message) =>
          val error = intercept[IllegalArgumentException](WorkflowGraphIDs.validate(content))
          assert(error.getMessage.contains(message))
        case None => WorkflowGraphIDs.validate(content)
      }
      assert(content.toString == before)
    }

  for (group <- List("positive", "negative"); example <- matrix.path(group).elements().asScala)
    check(
      example.path("name").asText(),
      example.path("content"),
      Option(example.get("error")).map(_.asText())
    )

  private def reject(name: String, graph: Map[String, Any], error: String): Unit =
    check(
      name,
      objectMapper.valueToTree[JsonNode](graph),
      Some(matrix.path("errors").path(error).asText())
    )

  for (((field, key), i) <- collections.zipWithIndex) {
    for ((other, otherKey) <- collections.drop(i)) {
      val ids = List(field -> Map(key -> "same"), other -> Map(otherKey -> "same"))
      reject(s"reject $field/$other collisions", ids.groupMap(_._1)(_._2), "collision")
    }
    for (invalid <- matrix.path("invalidIDs").elements().asScala)
      reject(
        s"reject $field ID ${invalid.path("label").asText()}",
        Map(field -> List(Map(key -> invalid.path("value")))),
        "id"
      )
    reject(s"reject malformed $field", Map(field -> Map.empty[String, String]), "graph")
  }
}
