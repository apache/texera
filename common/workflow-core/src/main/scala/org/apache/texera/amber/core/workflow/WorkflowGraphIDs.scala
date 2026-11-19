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
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Canvas cells share an ID namespace; each pinned graph owns a separate namespace. */
object WorkflowGraphIDs {
  def validateIDs(ids: Iterable[String], path: String = "Workflow"): Unit = {
    val seen = mutable.HashSet.empty[String]
    ids.foreach { id =>
      require(id != null && id.trim.nonEmpty, s"$path: Invalid graph element ID")
      require(seen.add(id), s"$path: Duplicate graph element ID '$id'")
    }
  }

  def validate(content: JsonNode, path: String = "Workflow", depth: Int = 0): Unit = {
    require(content != null && content.isObject && depth <= 32, s"$path: Malformed workflow graph")
    val ids =
      List("operators" -> "operatorID", "links" -> "linkID", "commentBoxes" -> "commentBoxID")
        .flatMap {
          case (field, key) =>
            val records = content.path(field)
            require(
              records.isMissingNode || records.isNull || records.isArray,
              s"$path: Malformed workflow graph"
            )
            records.elements().asScala.map(_.path(key).textValue()).toList
        }
    validateIDs(ids, path)
    val dependencies = content.path("macroDependencies")
    require(
      dependencies.isMissingNode || dependencies.isNull || dependencies.isArray,
      s"$path: Malformed workflow graph"
    )
    dependencies.elements().asScala.foreach { definition =>
      validate(
        definition.path("content"),
        s"$path / ${definition.path("name").asText("Macro")}",
        depth + 1
      )
    }
  }
}
