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

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.texera.amber.core.state.{State, StateReferencing}
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

/**
  * `StateReferenceBinding`, driven the way the worker drives it: `registerState` for each state
  * message, then `bindStateReferences` before the first tuple, on an executor `ExecFactory` built.
  */
class StateReferenceBindingSpec extends AnyFlatSpec {

  import StateReferenceBindingSpec._

  private val allReferences = Map(
    "/name" -> "i",
    "/limit" -> "n",
    "/tags/1" -> "t",
    "/predicates/1/value" -> "v",
    "/predicates/1/threshold" -> "h"
  )

  /** The descString as the compiler hands it over: placeholders, and the sidecar naming them. */
  private def descString(references: Map[String, String] = allReferences): String =
    s"""{"name":"$$i","limit":0,"ratio":0.0,"enabled":false,"tags":["a","$$t"],
       |"predicates":[{"attribute":"x","value":"kept","threshold":1.5,"count":0},
       |{"attribute":"y","value":"$$v","threshold":0.0,"count":0}],
       |"stateReferences":${objectMapper.writeValueAsString(references)}}""".stripMargin

  private def build(references: Map[String, String] = allReferences): SettingExec =
    ExecFactory
      .newExecFromJavaClassName(classOf[SettingExec].getName, descString(references))
      .asInstanceOf[SettingExec]

  private def missing(pointer: String, name: String): String =
    s"property $pointer refers to loop variable $name, but no state message carried it"

  "StateReferenceBinding" should "write each variable a registered message carries into the setting the executor holds, in place" in {
    val exec = build()
    val setting = exec.setting
    val kept = setting.predicates.head
    exec.registerState(State(Map("n" -> 2L, "v" -> "x1", "h" -> 2.5, "unrelated" -> 1)))

    // The executor keeps reading the very object its constructor parsed.
    assert(exec.setting eq setting)
    assert(setting.limit == 2)
    assert(setting.predicates.map(_.value) == List("kept", "x1"))
    assert(setting.predicates.map(_.threshold) == List(1.5, 2.5))
    assert(setting.predicates.head.attribute == "x" && setting.predicates(1).attribute == "y")
    // A property the message does not carry keeps its placeholder, and so does everything else.
    assert(setting.name == "$i")
    assert(setting.tags == List("a", "$t"))
    assert((setting.ratio, setting.enabled) == ((0.0, false)))
    assert(kept.value == "kept")
    // The sidecar is what the executor's own parse recorded; nothing writes it.
    assert(setting.stateReferences.isEmpty)
  }

  it should "let a more deeply nested loop's value replace an outer one's, even one that did not fit, and keep it" in {
    // In a nested loop the inner body first receives the outer loop's state, one loop out, then
    // the inner one's: an inner variable shadows an outer one of the same name.
    val exec = build()
    val outer = State(Map("i" -> 1L, "n" -> 2, "t" -> "outer", "v" -> "o", "h" -> "not a number"))
    exec.registerState(outer, loopCounter = 1)
    exec.registerState(State(Map("i" -> 5L, "t" -> "inner", "h" -> 1)), loopCounter = 0)
    // A later copy of the outer loop's state, from another upstream worker, does not undo it.
    exec.registerState(outer, loopCounter = 1)
    exec.bindStateReferences()

    val setting = exec.setting
    assert((setting.name, setting.limit, setting.tags) == (("5", 2, List("a", "inner"))))
    assert(setting.predicates(1).value == "o")
    assert(setting.predicates(1).threshold == 1.0)
    assert(exec.state.contains(outer))
  }

  it should "accept copies from one loop that agree, and fail on one that gives a variable another value, bound or not" in {
    def conflict(earlier: Any, later: Any): String =
      s"loop variable n got two different values in one iteration, $earlier and $later: " +
        "a loop's variables must not change inside its body"
    val exec = build(Map("/limit" -> "n", "/name" -> "i"))
    // Copies from several upstream workers or branches: the same number whatever its boxed type,
    // and a variable no property refers to may differ.
    exec.registerState(State(Map("n" -> 1L, "unrelated" -> 1)), loopCounter = 1)
    val copy = State(Map("n" -> 1, "unrelated" -> 2))
    exec.registerState(copy, loopCounter = 1)

    assert(
      intercept[IllegalStateException](
        exec.registerState(State(Map("i" -> "x", "n" -> 2L)), loopCounter = 1)
      ).getMessage == conflict(1, 2)
    )
    // Nothing of the refused message is written, not even its i, which no message carried yet and
    // which sorts before n; nor is the message registered.
    assert((exec.setting.limit, exec.setting.name) == ((1, "$i")))
    assert(exec.state.contains(copy))
    exec.registerState(State(Map("i" -> "y")), loopCounter = 1)
    exec.bindStateReferences()
    assert(
      intercept[IllegalStateException](
        exec.registerState(State(Map("n" -> "x")), loopCounter = 1)
      ).getMessage == conflict(1, "x")
    )
    assert((exec.setting.limit, exec.setting.name) == ((1, "y")))
  }

  it should "fail at binding, naming every reference no state message carried, until one does" in {
    val exec = build()
    exec.registerState(State(Map("i" -> 1L)))
    val message = intercept[IllegalStateException](exec.bindStateReferences()).getMessage
    assert(
      message == Seq(
        missing("/limit", "n"),
        missing("/predicates/1/threshold", "h"),
        missing("/predicates/1/value", "v"),
        missing("/tags/1", "t")
      ).mkString("; ")
    )
    assert(exec.setting.name == "1")
    assert(exec.setting.limit == 0)
    // A failed binding does not end the writing.
    assert(intercept[IllegalStateException](exec.bindStateReferences()).getMessage == message)
    exec.registerState(State(Map("n" -> 4, "h" -> 1, "v" -> "x", "t" -> "y")))
    exec.bindStateReferences()
    assert(exec.setting.limit == 4)
  }

  it should "stop writing once bound: a later message is still registered, but the setting keeps its values" in {
    // Workers are recreated for each iteration, so a bound setting never needs rebinding.
    val exec = build(Map("/limit" -> "n", "/name" -> "i"))
    exec.registerState(State(Map("n" -> 2L, "i" -> "x")), loopCounter = 1)
    exec.bindStateReferences()
    // Even a more deeply nested loop's, which would have replaced them before.
    val later = State(Map("n" -> 9L, "i" -> "y"))
    exec.registerState(later, loopCounter = 0)
    exec.bindStateReferences()
    assert(exec.state.contains(later))
    assert((exec.setting.limit, exec.setting.name) == ((2, "x")))
  }

  it should "write a variable into every property that refers to it" in {
    val exec = build(Map("/name" -> "i", "/tags/1" -> "i", "/limit" -> "i"))
    exec.registerState(State(Map("i" -> 3L)))
    exec.bindStateReferences()
    assert((exec.setting.name, exec.setting.tags, exec.setting.limit) == (("3", List("a", "3"), 3)))
  }

  it should "report a sidecar pointer that names no value of the setting at binding" in {
    // "limit" is not a JSON pointer at all: it does not start with "/".
    Seq("/nothing", "/tags/5", "/predicates", "/predicates/1", "/stateReferences/x", "", "limit")
      .foreach { pointer =>
        assert(
          bindOneFails(pointer, 1) == s"property $pointer refers to loop variable k, but " +
            s"${classOf[SettingExec].getName} parsed a setting with no value there",
          pointer
        )
      }
  }

  // ---------------------------------------------------------------------------
  // Coercion: the placeholder's JSON type wins
  // ---------------------------------------------------------------------------

  private def bindOne(pointer: String, value: Any): Setting = {
    val exec = build(Map(pointer -> "k"))
    exec.registerState(State(Map("k" -> value)))
    exec.bindStateReferences()
    exec.setting
  }

  /** A value that cannot be written is reported at binding, not when its message arrives. */
  private def bindOneFails(pointer: String, value: Any): String = {
    val exec = build(Map(pointer -> "k"))
    exec.registerState(State(Map("k" -> value)))
    intercept[IllegalStateException](exec.bindStateReferences()).getMessage
  }

  it should "bind each placeholder from a value its JSON type accepts" in {
    def accepts[T](pointer: String, read: Setting => T)(cases: (Any, T)*): Unit =
      cases.foreach {
        case (value, expected) => assert(read(bindOne(pointer, value)) == expected, value)
      }
    accepts("/limit", _.limit)(7 -> 7, 7L -> 7, 7.0 -> 7, "7" -> 7, " 7 " -> 7)
    accepts("/ratio", _.ratio)(3 -> 3.0, 2.5 -> 2.5, "1.5" -> 1.5, 7L -> 7.0)
    accepts("/enabled", _.enabled)(true -> true, "false" -> false, "TRUE" -> true)
    accepts("/name", _.name)(5L -> "5", 2.5 -> "2.5", true -> "true", "text" -> "text", "" -> "")
    accepts("/tags/1", _.tags)("ünïcödé" -> List("a", "ünïcödé"))
  }

  it should "report a value its placeholder's JSON type does not accept, naming the property" in {
    Seq[(String, Any, String)](
      ("/limit", 7.5, "an integer"),
      ("/limit", "seven", "an integer"),
      ("/limit", true, "an integer"),
      ("/limit", "", "an integer"),
      ("/ratio", "abc", "a number"),
      ("/ratio", false, "a number"),
      ("/enabled", 1, "a boolean"),
      ("/enabled", "yes", "a boolean"),
      // A Python None arrives as null; a list, a map or bytes has no text to bind.
      ("/name", null, "a scalar"),
      ("/name", List("a", "b"), "a scalar"),
      ("/name", Map("a" -> 1), "a scalar"),
      ("/name", Array[Byte](1, 2), "a scalar")
    ).foreach {
      case (pointer, value, kind) =>
        assert(
          bindOneFails(pointer, value) ==
            s"property $pointer refers to loop variable k, but its value $value is not $kind"
        )
    }
  }

  it should "report an integer that does not fit the property, naming the property" in {
    val message = bindOneFails("/limit", 3000000000L)
    assert(
      message.startsWith(
        "property /limit refers to loop variable k, but its value 3000000000 does not fit it: "
      ),
      message
    )
  }

  it should "report a nested value that does not fit against its own property, and still write the ones after it" in {
    // Both fall under /predicates, which is handed to Jackson whole for each write: the value that
    // did not fit must not ride along with the next one.
    val exec = build(Map("/predicates/0/count" -> "c", "/predicates/1/value" -> "v"))
    exec.registerState(State(Map("c" -> 3000000000L, "v" -> "x1")))
    val message = intercept[IllegalStateException](exec.bindStateReferences()).getMessage
    assert(
      message.startsWith(
        "property /predicates/0/count refers to loop variable c, but its value 3000000000 does " +
          "not fit it: "
      ),
      message
    )
    assert(!message.contains("/predicates/1/value"), message)
    assert(exec.setting.predicates.map(_.value) == List("kept", "x1"))
    assert(exec.setting.predicates.map(_.count) == List(0, 0))
  }
}

private object StateReferenceBindingSpec {

  class Predicate {
    @JsonProperty var attribute: String = _
    @JsonProperty var value: String = _
    @JsonProperty var threshold: Double = _
    @JsonProperty var count: Int = _
  }

  /** One property of each JSON scalar type, a list of strings and a list of beans. */
  class Setting extends StateReferencing {
    @JsonProperty var name: String = _
    @JsonProperty var limit: Int = _
    @JsonProperty var ratio: Double = _
    @JsonProperty var enabled: Boolean = _
    @JsonProperty var tags: List[String] = List.empty
    @JsonProperty var predicates: List[Predicate] = List.empty
  }

  /** Parses its setting in the class body, as operator executors do. Public, for the factory. */
  class SettingExec(descString: String) extends OperatorExecutor {
    val setting: Setting = objectMapper.readValue(descString, classOf[Setting])
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.single(tuple)
  }
}
