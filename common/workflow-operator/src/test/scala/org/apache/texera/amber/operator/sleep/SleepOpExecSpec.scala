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

package org.apache.texera.amber.operator.sleep

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class SleepOpExecSpec extends AnyFlatSpec {

  private val schema: Schema = Schema().add(new Attribute("v", AttributeType.INTEGER))

  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(new Attribute("v", AttributeType.INTEGER), Integer.valueOf(v)).build()

  // SleepOpDesc is a LogicalOp: serialize a real instance so the polymorphic operatorType
  // discriminator is present (a hand-written JSON string would fail to deserialize).
  private def descString(sleepTime: Int): String = {
    val desc = new SleepOpDesc()
    desc.sleepTime = sleepTime
    objectMapper.writeValueAsString(desc)
  }

  "SleepOpExec" should "construct from a serialized SleepOpDesc" in {
    val exec = new SleepOpExec(descString(0))
    assert(exec != null)
  }

  "SleepOpExec.processTuple" should "return the input tuple unchanged" in {
    // sleepTime = 0 -> Thread.sleep(0), instant
    val exec = new SleepOpExec(descString(0))
    assert(exec.processTuple(tuple(7), 0).toList == List(tuple(7)))
  }

  it should "emit exactly one tuple per input" in {
    val exec = new SleepOpExec(descString(0))
    val emitted = (0 until 5).flatMap(i => exec.processTuple(tuple(i), 0).toList)
    assert(emitted == (0 until 5).map(tuple).toList)
  }

  // The one thing the operator is for. Every other case here configures no delay,
  // so the seconds-to-millis conversion was the only line nothing asked about:
  // dropping it, or losing the factor of a thousand, left them all passing.
  //
  // One second is the smallest delay the field can ask for, sleepTime being whole
  // seconds, and one tuple is enough to observe it. The lower bound is asserted
  // rather than a window: a loaded machine may take longer, and a delay that
  // overshoots is not the failure this guards against.
  it should "delay each tuple by the configured number of seconds" in {
    val exec = new SleepOpExec(descString(1))
    val startNanos = System.nanoTime()
    val emitted = exec.processTuple(tuple(1), 0).toList
    val elapsedMillis = (System.nanoTime() - startNanos) / 1000000
    assert(emitted == List(tuple(1)))
    assert(elapsedMillis >= 1000, s"expected at least 1000 ms of sleep, took $elapsedMillis ms")
  }
}
