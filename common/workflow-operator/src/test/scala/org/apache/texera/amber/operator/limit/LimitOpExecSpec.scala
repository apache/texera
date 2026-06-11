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

package org.apache.texera.amber.operator.limit

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class LimitOpExecSpec extends AnyFlatSpec {

  private val schema: Schema =
    Schema().add(new Attribute("v", AttributeType.INTEGER))

  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(new Attribute("v", AttributeType.INTEGER), Integer.valueOf(v)).build()

  // LogicalOp is registered for polymorphic Jackson deserialization via the
  // `operatorType` discriminator, so a hand-rolled `{"limit":N}` string would
  // fail to bind. Serialize a real `LimitOpDesc` instance to get the proper
  // discriminator embedded.
  private def desc(limit: Int): String = {
    val d = new LimitOpDesc()
    d.limit = limit
    objectMapper.writeValueAsString(d)
  }

  private def newExec(limit: Int): LimitOpExec = {
    val exec = new LimitOpExec(desc(limit))
    exec.open()
    exec
  }

  "LimitOpExec.processTuple" should "emit each input tuple while under the configured limit" in {
    val exec = newExec(3)
    val emitted = (0 until 3).flatMap(i => exec.processTuple(tuple(i), 0).toList).toList
    assert(emitted.map(_.asInstanceOf[Tuple]) == List(tuple(0), tuple(1), tuple(2)))
  }

  it should "emit nothing once the limit is reached" in {
    val exec = newExec(2)
    exec.processTuple(tuple(0), 0).toList
    exec.processTuple(tuple(1), 0).toList
    val third = exec.processTuple(tuple(2), 0).toList
    val fourth = exec.processTuple(tuple(3), 0).toList
    assert(third.isEmpty)
    assert(fourth.isEmpty)
  }

  it should "track the count cumulatively across processTuple invocations" in {
    val exec = newExec(5)
    val emitted = (0 until 7).flatMap(i => exec.processTuple(tuple(i), 0).toList)
    assert(emitted.size == 5)
    assert(exec.count == 5)
  }

  it should "emit nothing for limit = 0" in {
    val exec = newExec(0)
    val emitted = (0 until 4).flatMap(i => exec.processTuple(tuple(i), 0).toList)
    assert(emitted.isEmpty)
  }

  "LimitOpExec.open" should "reset the count to 0" in {
    val exec = new LimitOpExec(desc(3))
    exec.count = 99
    exec.open()
    assert(exec.count == 0)
  }
}
