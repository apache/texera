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

import org.scalatest.flatspec.AnyFlatSpec

class StateStorageSpec extends AnyFlatSpec {

  "StateStorage" should "round-trip a state and its loop_counter through a tuple" in {
    val state = State(Map("i" -> 2L, "name" -> "outer"))
    val tuple = StateStorage.toTuple(state, 3L)
    val (decodedState, decodedCounter) = StateStorage.fromTuple(tuple)
    assert(decodedState == state)
    assert(decodedCounter == 3L)
  }

  it should "materialize loop_counter as its own column, never inside content" in {
    // The whole point of the off-State design: loop_counter lives in a
    // sibling column, so the user state JSON in `content` stays clean.
    val state = State(Map("i" -> 7L))
    val tuple = StateStorage.toTuple(state, 5L)
    assert(tuple.getField[String]("content") == state.toJson)
    assert(!tuple.getField[String]("content").contains("loop_counter"))
    assert(tuple.getField[java.lang.Long]("loop_counter").toLong == 5L)
  }

  it should "default an absent counter round-trip to the written value" in {
    val tuple = StateStorage.toTuple(State(Map.empty), 0L)
    assert(tuple.getSchema == StateStorage.schema)
    val (decodedState, decodedCounter) = StateStorage.fromTuple(tuple)
    assert(decodedState == State(Map.empty))
    assert(decodedCounter == 0L)
  }
}
