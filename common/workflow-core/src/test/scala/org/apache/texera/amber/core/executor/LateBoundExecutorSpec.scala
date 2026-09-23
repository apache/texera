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

import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple, TupleLike}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ListBuffer

/**
  * `LateBoundExecutor` is what a worker holds for an operator whose descriptor's
  * `stateReferences` sidecar names loop variables. The tests drive it the way the worker does --
  * `open()` first, then state messages, then tuples -- against a small executor that parses a
  * descriptor with one property of each JSON scalar type.
  */
class LateBoundExecutorSpec extends AnyFlatSpec {

  import LateBoundExecutorSpec._

  private val schema: Schema = Schema().add(new Attribute("v", AttributeType.INTEGER))
  private def tuple(v: Int): Tuple =
    Tuple.builder(schema).add(schema.getAttribute("v"), Integer.valueOf(v)).build()

  /** The descriptor as the compiler hands it over: placeholders, and the sidecar naming them. */
  private def descOf(desc: Desc): String = objectMapper.writeValueAsString(desc)

  private val references = Map("/name" -> "i", "/limit" -> "n", "/tags/1" -> "t")
  private val descString = descOf(
    Desc(name = "$i", limit = 0, tags = List("a", "$t"), stateReferences = references)
  )

  private def lateBound(desc: String = descString): LateBoundExecutor =
    ExecFactory
      .newExecFromJavaClassName(classOf[RecordingExec].getName, desc, idx = 2, workerCount = 3)
      .asInstanceOf[LateBoundExecutor]

  private def bound(exec: LateBoundExecutor): RecordingExec =
    exec.boundExecutor.get.asInstanceOf[RecordingExec]

  "ExecFactory" should "hand out a LateBoundExecutor exactly when the sidecar names a loop variable" in {
    // Outside every loop block the sidecar is empty, and "$AAPL" is the literal the operator
    // compares against, as it was before loop variables existed.
    Seq("""{"name":"$AAPL"}""", """{"name":"$AAPL","stateReferences":{}}""").foreach { desc =>
      val exec = ExecFactory.newExecFromJavaClassName(classOf[WorkerAwareExec].getName, desc)
      assert(exec.asInstanceOf[WorkerAwareExec].desc == desc)
    }
    assert(lateBound().references == references)
  }

  "LateBoundExecutor" should "bind at the first tuple, from the merged state messages, the last one winning" in {
    // In a nested loop the inner body first receives the outer loop's state, then the inner one.
    // Both arrive before any tuple, so an inner variable shadows an outer one of the same name,
    // even when the outer state alone already carries every referenced variable.
    val exec = lateBound()
    exec.open()
    val outer = State(Map("i" -> 1L, "n" -> 2, "t" -> "outer"))
    assert(exec.processState(outer, 0).contains(outer))
    val inner = State(Map("i" -> 5L, "t" -> "inner"))
    assert(exec.processState(inner, 0).contains(inner))
    assert(exec.boundExecutor.isEmpty)

    assert(exec.processTuple(tuple(1), 0).toList == List(tuple(1)))
    val real = bound(exec)
    assert((real.desc.name, real.desc.limit, real.desc.tags) == ("5", 2, List("a", "inner")))
    assert(real.opened == 1)
    // The real executor's callback sees the same states, in order, as it would without the
    // references (If routes on them); they were already forwarded, as received.
    assert(real.states.toList == List(outer, inner))
  }

  it should "never rebind: later state messages go to the real executor's callback" in {
    val exec = lateBound()
    val first = State(Map("i" -> 1L, "n" -> 2L, "t" -> "x"))
    exec.processState(first, 0)
    exec.processTuple(tuple(1), 0)
    val real = bound(exec)
    val later = State(Map("i" -> 9L, "n" -> 9L, "t" -> "y"))
    assert(exec.processState(later, 0).contains(State(Map("seen" -> 3))))
    exec.processTuple(tuple(2), 0)
    assert(bound(exec) eq real)
    assert(real.desc.limit == 2)
    assert(real.states.toList == List(first, later))
    assert(real.opened == 1)
  }

  it should "refuse tuples and finishing while unbound, naming only the missing variables" in {
    val exec = lateBound()
    exec.processState(State(Map("i" -> 1L)), 0)
    Seq[() => Any](
      () => exec.processTuple(tuple(1), 0),
      () => exec.processTupleMultiPort(tuple(1), 0),
      () => exec.onFinish(0),
      () => exec.onFinishMultiPort(0),
      () => exec.produceStateOnFinish(0)
    ).foreach { call =>
      val message = intercept[IllegalStateException](call()).getMessage
      assert(
        message == "property /limit refers to loop variable n, but no state message carried it; " +
          "property /tags/1 refers to loop variable t, but no state message carried it"
      )
    }
  }

  it should "do nothing on open, close and produceStateOnStart until bound" in {
    val exec = lateBound()
    exec.open()
    exec.processState(State(Map("i" -> 5L, "n" -> 5L, "t" -> "x")), 0)
    assert(exec.produceStateOnStart(0).isEmpty)
    exec.close()
    assert(exec.boundExecutor.isEmpty)
  }

  it should "bind when a port finishes before any tuple arrived" in {
    val exec = lateBound()
    exec.processState(State(Map("i" -> 5L, "n" -> 5L, "t" -> "x")), 0)
    assert(exec.produceStateOnFinish(0).contains(State(Map("done" -> "5"))))
    assert(bound(exec).opened == 1)
  }

  it should "delegate every call once bound, and never reopen the real executor" in {
    val exec = lateBound()
    exec.processState(State(Map("i" -> 5L, "n" -> 5L, "t" -> "x")), 0)
    assert(exec.processTuple(tuple(7), 0).toList == List(tuple(7)))
    val real = bound(exec)
    exec.open()
    assert(real.opened == 1)
    assert(exec.processTupleMultiPort(tuple(8), 1).toList == List((tuple(8), None)))
    assert(real.tuples.toList == List((7, 0), (8, 1)))
    assert(exec.processState(State(Map("x" -> 1)), 0).contains(State(Map("seen" -> 1))))
    assert(exec.produceStateOnStart(0).contains(State(Map("start" -> "5"))))
    assert(exec.produceStateOnFinish(0).contains(State(Map("done" -> "5"))))
    assert(exec.onFinish(0).toList == List(tuple(5)))
    assert(exec.onFinishMultiPort(0).toList == List((tuple(5), None)))
    exec.close()
    assert(real.closed == 1)
  }

  it should "build the real executor through the factory's constructor path, with the worker index and count" in {
    val exec = ExecFactory
      .newExecFromJavaClassName(
        classOf[WorkerAwareExec].getName,
        """{"name":"$i","stateReferences":{"/name":"i"}}""",
        idx = 2,
        workerCount = 3
      )
      .asInstanceOf[LateBoundExecutor]
    exec.processState(State(Map("i" -> "bound")), 0)
    assert(exec.processTuple(tuple(1), 0).isEmpty)
    val real = exec.boundExecutor.get.asInstanceOf[WorkerAwareExec]
    assert(real.desc == """{"name":"bound"}""")
    assert((real.idx, real.workerCount) == (2, 3))
  }

  it should "surface the cause when the real executor's constructor fails" in {
    val exec = ExecFactory
      .newExecFromJavaClassName(
        classOf[RefusingExec].getName,
        """{"name":"$i","stateReferences":{"/name":"i"}}"""
      )
      .asInstanceOf[LateBoundExecutor]
    exec.processState(State(Map("i" -> "bad")), 0)
    // It stays unbound, so every call that needs the real executor surfaces the cause again.
    Seq[() => Any](() => exec.processTuple(tuple(1), 0), () => exec.onFinish(0)).foreach { call =>
      assert(intercept[IllegalArgumentException](call()).getMessage == "constructor refused bad")
    }
    assert(exec.boundExecutor.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Coercion: the placeholder's JSON type wins
  // ---------------------------------------------------------------------------

  private def bindOne(pointer: String, value: Any): Desc = {
    val exec = lateBound(descOf(Desc(name = "$k", stateReferences = Map(pointer -> "k"))))
    exec.processState(State(Map("k" -> value)), 0)
    exec.onFinish(0)
    bound(exec).desc
  }

  private def bindOneFails(pointer: String, value: Any): String =
    intercept[IllegalStateException](bindOne(pointer, value)).getMessage

  it should "bind an integer placeholder from an int, a long, a whole double or a numeric string" in {
    Seq[Any](7, 7L, 7.0, "7").foreach(value => assert(bindOne("/limit", value).limit == 7, value))
    Seq[Any](7.5, "seven", true).foreach { value =>
      assert(
        bindOneFails("/limit", value) ==
          s"property /limit refers to loop variable k, but its value $value is not an integer"
      )
    }
  }

  it should "bind a number placeholder from any number or a numeric string" in {
    Seq[(Any, Double)](3 -> 3.0, 2.5 -> 2.5, "1.5" -> 1.5).foreach {
      case (value, expected) => assert(bindOne("/ratio", value).ratio == expected)
    }
    Seq[Any]("abc", false).foreach { value =>
      assert(bindOneFails("/ratio", value).endsWith(s"its value $value is not a number"))
    }
  }

  it should "bind a boolean placeholder from a boolean or 'true' / 'false' text only" in {
    assert(bindOne("/enabled", true).enabled)
    assert(!bindOne("/enabled", "false").enabled)
    assert(bindOne("/enabled", "TRUE").enabled)
    Seq[Any](1, "yes").foreach { value =>
      assert(bindOneFails("/enabled", value).endsWith(s"its value $value is not a boolean"))
    }
  }

  it should "bind a string property from the text of a string, a number or a boolean only" in {
    Seq[(Any, String)](5L -> "5", 2.5 -> "2.5", true -> "true", "text" -> "text").foreach {
      case (value, expected) => assert(bindOne("/name", value).name == expected)
    }
    // A Python None arrives as null; a list, a map or bytes has no text to bind.
    Seq[Any](null, List("a", "b"), Map("a" -> 1), Array[Byte](1, 2)).foreach { value =>
      assert(bindOneFails("/name", value).endsWith(s"its value $value is not a scalar"))
    }
  }
}

private object LateBoundExecutorSpec {

  /** A descriptor with one property of each JSON scalar type, a list, and the sidecar. */
  case class Desc(
      name: String,
      limit: Int = 0,
      ratio: Double = 0.0,
      enabled: Boolean = false,
      tags: List[String] = List.empty,
      stateReferences: Map[String, String] = Map.empty
  )

  /** Parses `Desc` from its descString and records every call. Public, for the factory's reflection. */
  class RecordingExec(descString: String) extends OperatorExecutor {
    val desc: Desc = objectMapper.readValue(descString, classOf[Desc])
    var opened = 0
    var closed = 0
    val states: ListBuffer[State] = ListBuffer.empty
    val tuples: ListBuffer[(Int, Int)] = ListBuffer.empty
    private val schema: Schema = Schema().add(new Attribute("v", AttributeType.INTEGER))

    override def open(): Unit = opened += 1
    override def close(): Unit = closed += 1
    override def produceStateOnStart(port: Int): Option[State] =
      Some(State(Map("start" -> desc.name)))
    override def processState(state: State, port: Int): Option[State] = {
      states += state
      Some(State(Map("seen" -> state.values.size)))
    }
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
      tuples += ((tuple.getField[Integer]("v").intValue(), port))
      Iterator.single(tuple)
    }
    override def produceStateOnFinish(port: Int): Option[State] =
      Some(State(Map("done" -> desc.name)))
    override def onFinish(port: Int): Iterator[TupleLike] =
      Iterator.single(
        Tuple.builder(schema).add(schema.getAttribute("v"), Integer.valueOf(desc.limit)).build()
      )
  }

  /** Only a `(String, Int, Int)` constructor: the factory's fallback branch. */
  class WorkerAwareExec(val desc: String, val idx: Int, val workerCount: Int)
      extends OperatorExecutor {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
  }

  /** Its constructor refuses every descriptor, naming the bound `name`. */
  class RefusingExec(descString: String) extends OperatorExecutor {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
    throw new IllegalArgumentException(
      "constructor refused " + objectMapper.readTree(descString).get("name").asText()
    )
  }
}
