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

package org.apache.texera.amber.operator.ifStatement

import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class IfOpExecSpec extends AnyFlatSpec {

  private val schema: Schema =
    Schema().add(new Attribute("v", AttributeType.INTEGER))

  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(new Attribute("v", AttributeType.INTEGER), Integer.valueOf(v)).build()

  // The IfOpDesc requires polymorphic Jackson (operatorType discriminator),
  // so build a real instance and serialize it.
  private def desc(conditionName: String): String = {
    val d = new IfOpDesc()
    d.conditionName = conditionName
    objectMapper.writeValueAsString(d)
  }

  private val truePortId = PortIdentity(1)
  private val falsePortId = PortIdentity()

  "IfOpExec.processState" should "route to the true port when the condition value is true" in {
    val exec = new IfOpExec(desc("flag"))
    val result = exec.processState(State(Map[String, Any]("flag" -> true)), 0)
    assert(result.exists(_.values("flag") == true))

    val out = exec.processTupleMultiPort(tuple(1), 0).toList
    assert(out == List((tuple(1), Some(truePortId))))
  }

  it should "route to the false port when the condition value is false" in {
    val exec = new IfOpExec(desc("flag"))
    exec.processState(State(Map[String, Any]("flag" -> false)), 0)

    val out = exec.processTupleMultiPort(tuple(1), 0).toList
    assert(out == List((tuple(1), Some(falsePortId))))
  }

  "IfOpExec.processTupleMultiPort" should "default to the true port before any state is observed" in {
    val exec = new IfOpExec(desc("flag"))
    val out = exec.processTupleMultiPort(tuple(7), 0).toList
    assert(out == List((tuple(7), Some(truePortId))))
  }

  it should "reflect the most recent processState routing decision" in {
    val exec = new IfOpExec(desc("flag"))
    exec.processState(State(Map[String, Any]("flag" -> true)), 0)
    exec.processState(State(Map[String, Any]("flag" -> false)), 0)
    val out = exec.processTupleMultiPort(tuple(1), 0).toList
    assert(out == List((tuple(1), Some(falsePortId))))
  }

  "IfOpExec.processTuple" should "be unimplemented (multi-port routing is required)" in {
    val exec = new IfOpExec(desc("flag"))
    assertThrows[NotImplementedError] {
      exec.processTuple(tuple(1), 0)
    }
  }

  "IfOpExec.processState" should "pass on a state without the condition and keep its route" in {
    // Inside a loop, an operator upstream that adds the condition in its processState sends the
    // loop's message on first, without the condition, and the change after it.
    val exec = new IfOpExec(desc("flag"))
    exec.processState(State(Map[String, Any]("flag" -> false)), 0)
    val without = State(Map[String, Any]("other" -> true))
    assert(exec.processState(without, 0).exists(_ eq without))
    val out = exec.processTupleMultiPort(tuple(1), 0).toList
    assert(out == List((tuple(1), Some(falsePortId))))
  }

  it should "route by a later state that carries the condition after one that does not" in {
    val exec = new IfOpExec(desc("flag"))
    exec.processState(State(Map[String, Any]("i" -> 0)), 0)
    exec.processState(State(Map[String, Any]("i" -> 0, "flag" -> false)), 0)
    val out = exec.processTupleMultiPort(tuple(1), 0).toList
    assert(out == List((tuple(1), Some(falsePortId))))
  }

  "IfOpExec.processTupleMultiPort" should "fail a tuple, naming the condition, while no state that arrived carried it" in {
    val exec = new IfOpExec(desc("flag"))
    exec.processState(State(Map[String, Any]("i" -> 0)), 0)
    exec.processState(State(Map[String, Any]("i" -> 1)), 0)
    val error = intercept[IllegalStateException](exec.processTupleMultiPort(tuple(1), 0).toList)
    assert(
      error.getMessage ==
        "the If's condition refers to loop variable flag, but no state message carried it"
    )
    // A later tuple fails too, until a state carries the condition.
    assertThrows[IllegalStateException](exec.processTupleMultiPort(tuple(2), 0).toList)
    exec.processState(State(Map[String, Any]("flag" -> true)), 0)
    assert(exec.processTupleMultiPort(tuple(3), 0).toList == List((tuple(3), Some(truePortId))))
  }

  it should "fail a tuple, naming the value and its type, after a condition that is not a Boolean" in {
    val exec = new IfOpExec(desc("flag"))
    assertThrows[ClassCastException] {
      exec.processState(State(Map[String, Any]("flag" -> "true")), 0)
    }
    // A state carried the condition, so the error names its value rather than a missing state.
    val error = intercept[IllegalStateException](exec.processTupleMultiPort(tuple(1), 0).toList)
    assert(
      error.getMessage ==
        "the If's condition refers to loop variable flag, but its value true, a String, is not " +
          "a Boolean"
    )
  }

  it should "throw ClassCastException when the conditionName value is a String" in {
    val exec = new IfOpExec(desc("flag"))
    // Current contract is `asInstanceOf[Boolean]`, so a non-Boolean value
    // must surface as a ClassCastException rather than a silent route.
    assertThrows[ClassCastException] {
      exec.processState(State(Map[String, Any]("flag" -> "yes")), 0)
    }
  }

  it should "throw ClassCastException when the conditionName value is an Int" in {
    val exec = new IfOpExec(desc("flag"))
    // Scala's `asInstanceOf[Boolean]` does not coerce numerics to booleans
    // the way Python's `bool(1)` does — it must throw.
    assertThrows[ClassCastException] {
      exec.processState(State(Map[String, Any]("flag" -> 1)), 0)
    }
  }

  it should "throw ClassCastException when the conditionName value is zero (Int)" in {
    val exec = new IfOpExec(desc("flag"))
    // Likewise, integer 0 is not coerced to false.
    assertThrows[ClassCastException] {
      exec.processState(State(Map[String, Any]("flag" -> 0)), 0)
    }
  }

  it should "treat a null condition value as false (default Boolean unbox)" in {
    val exec = new IfOpExec(desc("flag"))
    // `null.asInstanceOf[Boolean]` quietly unboxes to `false` in Scala, so
    // a null condition routes to the FALSE port rather than throwing. This
    // is a quiet behavior worth pinning so a future change to throw doesn't
    // silently regress.
    val result = exec.processState(State(Map[String, Any]("flag" -> null)), 0)
    assert(result.isDefined)
    val out = exec.processTupleMultiPort(tuple(1), 0).toList
    assert(out == List((tuple(1), Some(falsePortId))))
  }
}
