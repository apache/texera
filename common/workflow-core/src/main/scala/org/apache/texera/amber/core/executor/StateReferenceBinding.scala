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

package org.apache.texera.amber.core.executor

import com.fasterxml.jackson.core.{JsonPointer, JsonProcessingException}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}
import org.apache.texera.amber.core.state.StateReferencing.SIDECAR_PROPERTY
import org.apache.texera.amber.core.state.{State, StateReferencing}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Try

/**
  * The loop-variable references of one executor's setting: the descriptor its constructor parsed
  * from its descString, whose `stateReferences` sidecar maps the JSON pointer of each placeholder
  * to a loop variable.
  *
  * Each state message registered on the executor writes every referenced variable it carries into
  * the setting, in place, so the executor keeps reading the object it holds; the value is coerced
  * to the placeholder's JSON type. A later message's value replaces an earlier one's, so in a
  * nested loop, whose inner body receives the outer loop's state first, an inner variable shadows
  * an outer one of the same name. A value that cannot be written leaves the property as it was and
  * is reported by `unbound`, unless a later message's value can be.
  *
  * @param className the executor's class, named in the errors.
  */
private[executor] final class StateReferenceBinding(
    className: String,
    setting: StateReferencing,
    references: Map[String, String]
) {

  /** The setting's own JSON, the same values the executor reads, updated with each write. */
  private val tree: ObjectNode = {
    val json = objectMapper.valueToTree[ObjectNode](setting)
    json.remove(SIDECAR_PROPERTY)
    json
  }

  /** The placeholder at each pointer, whose JSON type each value is coerced to. */
  private val placeholders: Map[String, JsonNode] = references.map {
    case (pointer, name) =>
      val placeholder = Try(tree.at(pointer)).toOption.filter(_.isValueNode)
      pointer -> placeholder.getOrElse(
        throw new IllegalStateException(
          s"property $pointer refers to loop variable $name, but $className parsed a setting " +
            "with no value there"
        )
      )
  }

  /** Why each pointer is not written yet; a pointer is removed once a value is written to it. */
  private val problems: mutable.SortedMap[String, String] = mutable.SortedMap.from(references.map {
    case (pointer, name) =>
      pointer -> s"property $pointer refers to loop variable $name, but no state message carried it"
  })

  /** Writes each referenced variable `state` carries into the setting. */
  def write(state: State): Unit =
    references.toSeq.sorted.foreach {
      case (pointer, name) =>
        state.values.get(name).foreach { value =>
          try {
            writeAt(
              pointer,
              StateReferenceBinding.coerce(placeholders(pointer), value, pointer, name)
            )
            problems -= pointer
          } catch {
            case e: IllegalStateException => problems(pointer) = e.getMessage
            case e: JsonProcessingException =>
              problems(pointer) = s"property $pointer refers to loop variable $name, but its " +
                s"value $value does not fit it: ${e.getOriginalMessage}"
          }
        }
    }

  /** Why each reference is not written yet, in pointer order: empty once every one is. */
  def unbound: Seq[String] = problems.values.toSeq

  /**
    * Puts `value` at `pointer` in the setting's JSON, and hands the top-level property it falls
    * under back to Jackson, which updates the setting in place; the property is replaced whole,
    * the setting itself is not. When Jackson refuses the value, the JSON is restored.
    */
  private def writeAt(pointer: String, value: JsonNode): Unit = {
    val path = JsonPointer.compile(pointer)
    val parent = tree.at(path.head)
    val previous = tree.at(path)
    replace(parent, path.last, value)
    val property = path.getMatchingProperty
    try {
      objectMapper
        .readerForUpdating(setting)
        .readValue[AnyRef](
          JsonNodeFactory.instance.objectNode().set[ObjectNode](property, tree.get(property))
        )
    } catch {
      case e: JsonProcessingException =>
        replace(parent, path.last, previous)
        throw e
    }
  }

  private def replace(parent: JsonNode, last: JsonPointer, value: JsonNode): Unit =
    parent match {
      case obj: ObjectNode  => obj.replace(last.getMatchingProperty, value)
      case array: ArrayNode => array.set(last.getMatchingIndex, value)
      case _                => throw new IllegalStateException(s"no container above $last")
    }
}

private[executor] object StateReferenceBinding {

  private val nodes = JsonNodeFactory.instance

  /** The `stateReferences` sidecar of `descString`: empty unless it is a descriptor naming one. */
  def sidecarOf(descString: String): Map[String, String] =
    Try(objectMapper.readTree(descString)).toOption
      .collect { case tree: ObjectNode => tree }
      .flatMap(tree => Option(tree.get(SIDECAR_PROPERTY)))
      .iterator
      .flatMap(_.fields().asScala)
      .map(entry => entry.getKey -> entry.getValue.asText())
      .toMap

  /** The node that takes `placeholder`'s place for `value`: the placeholder's JSON type wins. */
  private def coerce(placeholder: JsonNode, value: Any, pointer: String, name: String): JsonNode = {
    val text = String.valueOf(value).trim
    def fail(kind: String): Nothing =
      throw new IllegalStateException(
        s"property $pointer refers to loop variable $name, but its value $value is not $kind"
      )
    def as[T](kind: String)(convert: => T): T = Try(convert).getOrElse(fail(kind))
    if (placeholder.isIntegralNumber) {
      nodes.numberNode(as("an integer")(new java.math.BigDecimal(text).longValueExact()))
    } else if (placeholder.isNumber) {
      nodes.numberNode(as("a number")(text.toDouble))
    } else if (placeholder.isBoolean) {
      nodes.booleanNode(as("a boolean")(text.toBooleanOption.get))
    } else {
      value match {
        case _: String | _: java.lang.Number | _: java.lang.Boolean =>
          nodes.textNode(value.toString)
        case _ => fail("a scalar")
      }
    }
  }
}
