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

import com.fasterxml.jackson.databind.JsonNode
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.util.Base64
import scala.jdk.CollectionConverters.IteratorHasAsScala

final case class State(values: Map[String, Any]) {

  def toJson: String =
    objectMapper.writeValueAsString(State.toJsonValue(values))

  def toTuple(
      loopCounter: Long = 0L,
      loopStartId: String = ""
  ): Tuple =
    Tuple
      .builder(State.schema)
      .addSequentially(Array(toJson, Long.box(loopCounter), loopStartId))
      .build()
}

object State {

  // Reserved key marking a State as an in-band error signal (an operator
  // failure traveling forward as a dataflow event). The value is an envelope
  // map: {operatorId, workerId, errorType, message}. Recognition and port
  // poisoning happen in the worker (DataProcessor); operators are oblivious
  // unless they explicitly consume it (try/catch frame operators do).
  val ErrorKey = "__error__"
  private val ErrOperatorId = "operatorId"

  def errorState(operatorId: String, workerId: String, e: Throwable): State =
    State(
      Map(
        ErrorKey -> Map(
          ErrOperatorId -> operatorId,
          "workerId" -> workerId,
          "errorType" -> e.getClass.getName,
          "message" -> Option(e.getMessage).getOrElse("")
        )
      )
    )

  def isError(state: State): Boolean = state.values.contains(ErrorKey)

  /** The canonical physical-operator id of the failing operator, if this is an error State. */
  def errorOperatorId(state: State): Option[String] =
    state.values.get(ErrorKey) match {
      case Some(envelope: Map[_, _]) =>
        envelope.asInstanceOf[Map[String, Any]].get(ErrOperatorId).map(_.toString)
      case _ => None
    }

  private val Content = "content"
  // loop-control bookkeeping owned by the (Python) worker runtime; not user
  // state and never in the content JSON. Materialized as its own columns,
  // parallel to content. Scala never ORIGINATES loop state (loop operators are
  // Python-only), so toTuple defaults these to the "no loop" values -- but a
  // JVM operator inside a loop body must CARRY them through unchanged, so the
  // extractors below read them back off a materialized/transported row.
  private val LoopCounter = "loop_counter"
  private val LoopStartId = "loop_start_id"

  /** Read the loop-envelope counter off a State row (see `toTuple`). */
  def loopCounterFrom(row: Tuple): Long = row.getField[java.lang.Long](LoopCounter).longValue()

  /** Read the loop-envelope LoopStart id off a State row (see `toTuple`). */
  def loopStartIdFrom(row: Tuple): String = row.getField[String](LoopStartId)
  private val BytesTypeMarker = "__texera_type__"
  private val BytesValue = "bytes"
  private val PayloadMarker = "payload"

  val schema: Schema = new Schema(
    new Attribute(Content, AttributeType.STRING),
    new Attribute(LoopCounter, AttributeType.LONG),
    new Attribute(LoopStartId, AttributeType.STRING)
  )

  def fromJson(payload: String): State =
    State(
      objectMapper
        .readTree(payload)
        .fields()
        .asScala
        .map(entry => entry.getKey -> fromJsonValue(entry.getValue))
        .toMap
    )

  def fromTuple(row: Tuple): State = fromJson(row.getField[String](Content))

  private def toJsonValue(value: Any): Any =
    value match {
      case null => null
      case bytes: Array[Byte] =>
        Map(BytesTypeMarker -> BytesValue, PayloadMarker -> Base64.getEncoder.encodeToString(bytes))
      case map: Map[?, ?] =>
        map.iterator.map { case (k, v) => k -> toJsonValue(v) }.toMap
      case iterable: Iterable[_] =>
        iterable.map(toJsonValue).toList
      case other => other
    }

  private def fromJsonValue(node: JsonNode): Any = {
    if (node == null || node.isNull) {
      null
    } else if (node.isObject) {
      val fields = node.fields().asScala.map(entry => entry.getKey -> entry.getValue).toMap
      fields.get(BytesTypeMarker) match {
        case Some(typeNode) if typeNode.isTextual && typeNode.asText() == BytesValue =>
          Base64.getDecoder.decode(fields(PayloadMarker).asText())
        case _ =>
          fields.view.mapValues(fromJsonValue).toMap
      }
    } else if (node.isArray) {
      node.elements().asScala.map(fromJsonValue).toList
    } else if (node.isBoolean) {
      node.asBoolean()
    } else if (node.isIntegralNumber) {
      node.longValue()
    } else if (node.isFloatingPointNumber) {
      node.doubleValue()
    } else {
      node.asText()
    }
  }
}
