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
  * Writes the loop variables each state message carries into one executor's setting, the
  * descriptor it holds, in place, at the placeholders its `stateReferences` sidecar names. Each
  * value is coerced to the placeholder's JSON type. A message from a more deeply nested loop (a
  * smaller `loopCounter`) replaces an outer one's value, so an inner loop's variable shadows an
  * outer one of the same name. Messages from the same loop are copies, from several upstream
  * workers or branches, so a different value is an error, bound or not. A value that cannot be
  * written leaves the property as it was and is reported by `bind`, unless a deeper one can be.
  *
  * @param className the executor's class, named in the errors.
  */
private[executor] final class StateReferenceBinding(
    className: String,
    setting: StateReferencing,
    references: Map[String, String]
) {

  /** Why each pointer is not written yet; a pointer is removed once a value is written to it. */
  private val problems: mutable.SortedMap[String, String] = mutable.SortedMap.from(references.map {
    case (pointer, name) =>
      pointer -> s"property $pointer refers to loop variable $name, but no state message carried it"
  })

  /** Each referenced variable's value so far, and the `loopCounter` of the message carrying it. */
  private val values = mutable.Map.empty[String, (Long, Any)]

  /** Set once bound: a later message is still checked, but no longer written. */
  private var bound = false

  /** Writes each referenced variable `state` carries into the setting, in pointer order. */
  def write(state: State, loopCounter: Long): Unit = {
    val carried =
      references.values.toSeq.distinct.sorted.flatMap(n => state.values.get(n).map(n -> _))
    for ((name, value) <- carried; (depth, earlier) <- values.get(name))
      if (depth == loopCounter && earlier != value)
        throw new IllegalStateException(
          s"loop variable $name got two different values in one iteration, $earlier and " +
            s"$value: a loop's variables must not change inside its body"
        )
    val replacing =
      if (bound) Map.empty[String, Any]
      else carried.filter { case (name, _) => values.get(name).forall(_._1 > loopCounter) }.toMap
    replacing.foreach { case (name, value) => values(name) = (loopCounter, value) }
    for ((pointer, name) <- references.toSeq.sorted; value <- replacing.get(name))
      try {
        writeAt(pointer, name, value)
        problems -= pointer
      } catch {
        case e: IllegalStateException => problems(pointer) = e.getMessage
        case e: JsonProcessingException =>
          problems(pointer) = s"property $pointer refers to loop variable $name, but its " +
            s"value $value does not fit it: ${e.getOriginalMessage}"
      }
  }

  /** Fails, naming in pointer order each reference not written yet, or ends the writing. */
  def bind(): Unit = {
    if (problems.nonEmpty) throw new IllegalStateException(problems.values.mkString("; "))
    bound = true
  }

  /**
    * Puts `value` at `pointer` in a fresh JSON of the setting, and hands the top-level property it
    * falls under back to Jackson, which replaces that property of the setting in place. Jackson
    * sets a property only once its whole value has parsed, so a refused value changes nothing.
    */
  private def writeAt(pointer: String, name: String, value: Any): Unit = {
    val json = objectMapper.valueToTree[ObjectNode](setting)
    val path = Try(JsonPointer.compile(pointer)).toOption
      .filter(json.at(_).isValueNode)
      .getOrElse(
        throw new IllegalStateException(
          s"property $pointer refers to loop variable $name, but $className parsed a setting " +
            "with no value there"
        )
      )
    val node = StateReferenceBinding.coerce(json.at(path), value, pointer, name)
    json.at(path.head) match {
      case obj: ObjectNode  => obj.replace(path.last.getMatchingProperty, node)
      case array: ArrayNode => array.set(path.last.getMatchingIndex, node)
    }
    objectMapper.readerForUpdating(setting).readValue[AnyRef](json.retain(path.getMatchingProperty))
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
