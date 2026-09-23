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

import com.fasterxml.jackson.core.JsonPointer
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, JsonNodeFactory, ObjectNode}
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.state.StateReferencing.SIDECAR_PROPERTY
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.lang.reflect.InvocationTargetException
import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.Try

/**
  * What a worker holds for an operator whose descriptor's `stateReferences` sidecar maps the JSON
  * pointer of each placeholder to a loop variable.
  *
  * Unbound, it keeps and forwards each state message. It binds when the real executor is first
  * needed, at a tuple or a finishing port: a channel carries its state messages ahead of its
  * tuples, so every message has arrived by then. Their values are merged, a later one winning, so
  * in a nested loop, whose inner body receives the outer loop's state first, an inner variable
  * shadows an outer one of the same name. Each placeholder takes its variable's value, coerced to
  * the placeholder's JSON type; the real executor is built through the normal factory path,
  * opened, and shown the kept messages in order, as it would have seen them without references
  * (what it returns for them is dropped: they were already forwarded as received). It is never
  * rebound: workers are recreated for each iteration.
  */
class LateBoundExecutor private[executor] (
    className: String,
    descString: String,
    idx: Int,
    workerCount: Int
) extends OperatorExecutor {

  private val tree = objectMapper.readTree(descString).asInstanceOf[ObjectNode]

  /** The JSON pointer of each placeholder -> the loop variable it refers to. */
  private[executor] val references: Map[String, String] = LateBoundExecutor.sidecar(tree)

  private val received = mutable.ArrayBuffer.empty[(State, Int)]
  private var real: Option[OperatorExecutor] = None

  private[executor] def boundExecutor: Option[OperatorExecutor] = real

  // The worker opens its executor before any state arrives; binding opens the real one.
  override def open(): Unit = ()

  override def close(): Unit = real.foreach(_.close())

  // A channel starts before any of its messages arrive, so there is nothing to bind from yet.
  override def produceStateOnStart(port: Int): Option[State] =
    real.flatMap(_.produceStateOnStart(port))

  override def produceStateOnFinish(port: Int): Option[State] =
    bound().produceStateOnFinish(port)

  override def processState(state: State, port: Int): Option[State] =
    real match {
      case Some(executor) => executor.processState(state, port)
      case None =>
        received += ((state, port))
        Some(state)
    }

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
    bound().processTuple(tuple, port)

  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = bound().processTupleMultiPort(tuple, port)

  override def onFinish(port: Int): Iterator[TupleLike] = bound().onFinish(port)

  override def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] =
    bound().onFinishMultiPort(port)

  private def bound(): OperatorExecutor =
    real.getOrElse {
      val values = received.foldLeft(Map.empty[String, Any])(_ ++ _._1.values)
      val missing = references.toSeq.sorted.collect {
        case (pointer, name) if !values.contains(name) =>
          s"property $pointer refers to loop variable $name, but no state message carried it"
      }
      if (missing.nonEmpty) throw new IllegalStateException(missing.mkString("; "))
      val executor = bind(values)
      real = Some(executor)
      received.foreach { case (state, port) => executor.processState(state, port) }
      received.clear()
      executor
    }

  private def bind(values: Map[String, Any]): OperatorExecutor = {
    val patched = tree.deepCopy()
    patched.remove(SIDECAR_PROPERTY) // so that the factory builds the real executor
    references.foreach {
      case (pointer, name) =>
        val path = JsonPointer.compile(pointer)
        val value = LateBoundExecutor.coerce(patched.at(path), values(name), pointer, name)
        patched.at(path.head) match {
          case parent: ObjectNode => parent.replace(path.last.getMatchingProperty, value)
          case parent: ArrayNode  => parent.set(path.last.getMatchingIndex, value)
          case _                  => throw new IllegalStateException(s"no property at $pointer")
        }
    }
    val desc = objectMapper.writeValueAsString(patched)
    val executor =
      try ExecFactory.newExecFromJavaClassName(className, desc, idx, workerCount)
      catch { case e: InvocationTargetException if e.getCause != null => throw e.getCause }
    executor.open()
    executor
  }
}

object LateBoundExecutor {

  private val nodes = JsonNodeFactory.instance

  /** Whether `descString` is a descriptor whose sidecar names a loop variable. */
  def refersToLoopVariables(descString: String): Boolean =
    Try(objectMapper.readTree(descString)).toOption.exists {
      case tree: ObjectNode => sidecar(tree).nonEmpty
      case _                => false
    }

  private def sidecar(tree: ObjectNode): Map[String, String] =
    Option(tree.get(SIDECAR_PROPERTY)).iterator
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
