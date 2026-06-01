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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}

/**
  * Two-column wire/storage format for a [[State]] plus its loop-control
  * `loop_counter`.
  *
  * `loop_counter` is loop bookkeeping owned by the (Python) worker runtime and
  * is intentionally NOT part of [[State]]. Whenever state is materialized (the
  * state storage table) or sent over the wire it is written as its own
  * `loop_counter` column parallel to `content`, so it never enters the user
  * state JSON. Scala operators never produce a non-zero counter, so the Scala
  * write paths emit `0`; this object exists so the bilingual state table schema
  * and tuple layout stay byte-for-byte in sync with the Python `StateStorage`.
  */
object StateStorage {
  val Content = "content"
  val LoopCounter = "loop_counter"

  val schema: Schema = new Schema(
    new Attribute(Content, AttributeType.STRING),
    new Attribute(LoopCounter, AttributeType.LONG)
  )

  def toTuple(state: State, loopCounter: Long): Tuple =
    Tuple
      .builder(schema)
      .addSequentially(Array(state.toJson, Long.box(loopCounter)))
      .build()

  def fromTuple(row: Tuple): (State, Long) =
    (
      State.fromJson(row.getField[String](Content)),
      row.getField[java.lang.Long](LoopCounter).toLong
    )
}
