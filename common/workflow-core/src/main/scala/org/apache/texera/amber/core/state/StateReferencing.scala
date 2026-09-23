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

package org.apache.texera.amber.core.state

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
  * A descriptor whose properties may refer to loop variables.
  *
  * Inside a loop block (LoopStart ... LoopEnd) a property whose WHOLE value is `$K` refers to the
  * loop variable `K`, which reaches the operator in the iteration's state message. The
  * `stateReferences` sidecar records each such property: its JSON pointer -> the variable name.
  * The parse fills it in for a typed property, which holds a placeholder instead
  * (`StateReferenceModule`); the compiler adds the string ones inside a loop block and rejects
  * every entry outside one (`WorkflowCompiler.normalizeStateReferences`). It rides in the
  * descriptor's JSON to the worker, which binds it (`LateBoundExecutor`), and the property panel
  * never shows it.
  */
trait StateReferencing {

  @JsonProperty(StateReferencing.SIDECAR_PROPERTY)
  var stateReferences: Map[String, String] = Map.empty
}

object StateReferencing {

  /** The JSON name of the sidecar. */
  final val SIDECAR_PROPERTY = "stateReferences"

  /** `$` followed by a name; the WHOLE string must match. */
  private val ReferencePattern = "^\\$([A-Za-z_][A-Za-z0-9_]*)$".r

  /** The loop variable `value` refers to, when the whole string is a `$name` reference. */
  def referencedVariable(value: String): Option[String] =
    value match {
      case ReferencePattern(name) => Some(name)
      case _                      => None
    }

  /** Escape one property name as an RFC 6901 pointer segment: `~` -> `~0`, `/` -> `~1`. */
  def escapePointerSegment(name: String): String =
    name.replace("~", "~0").replace("/", "~1")

  /** Every whole-string `$name` value of `tree` outside its sidecar: its JSON pointer -> name. */
  def literalReferences(tree: ObjectNode): Map[String, String] = {
    def scan(node: JsonNode, pointer: String): Iterator[(String, String)] =
      if (node.isTextual) {
        referencedVariable(node.asText()).map(pointer -> _).iterator
      } else if (node.isObject) {
        node.fields().asScala.flatMap { entry =>
          scan(entry.getValue, pointer + "/" + escapePointerSegment(entry.getKey))
        }
      } else {
        node.elements().asScala.zipWithIndex.flatMap {
          case (element, i) => scan(element, s"$pointer/$i")
        }
      }
    tree
      .fields()
      .asScala
      .filter(_.getKey != SIDECAR_PROPERTY)
      .flatMap(entry => scan(entry.getValue, "/" + escapePointerSegment(entry.getKey)))
      .toMap
  }
}
