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
  * `StateReferenceBinding` writes the loop variables of each registered state message into the
  * setting an executor parsed from its descString, where the descriptor's `stateReferences` sidecar
  * names them. The tests drive it the way the worker does, through the executor built by
  * `ExecFactory`: `registerState` for each state message, then `bindStateReferences` before the
  * first tuple, against a small executor that parses a setting with one property of each JSON
  * scalar type, a list of strings and a list of beans.
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

  "StateReferenceBinding" should "write each variable a registered message carries into the setting the executor parsed, in place" in {
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

  it should "let a later message's value replace an earlier one's, so an inner loop's variable shadows an outer one" in {
    // In a nested loop the inner body first receives the outer loop's state, then the inner one.
    // Both arrive before any tuple, so an inner variable shadows an outer one of the same name,
    // even when the outer state alone already carries every referenced variable.
    val exec = build()
    exec.registerState(State(Map("i" -> 1L, "n" -> 2, "t" -> "outer", "v" -> "o", "h" -> 1)))
    exec.registerState(State(Map("i" -> 5L, "t" -> "inner")))
    exec.bindStateReferences()

    val setting = exec.setting
    assert((setting.name, setting.limit, setting.tags) == (("5", 2, List("a", "inner"))))
    assert(setting.predicates(1).value == "o")
    assert(setting.predicates(1).threshold == 1.0)
    assert(exec.state.contains(State(Map("i" -> 5L, "t" -> "inner"))))
  }

  it should "shadow an outer value that does not fit the property with an inner one that does" in {
    val exec = build(Map("/limit" -> "n"))
    exec.registerState(State(Map("n" -> "not a number")))
    assert(exec.setting.limit == 0)
    exec.registerState(State(Map("n" -> 3)))
    exec.bindStateReferences()
    assert(exec.setting.limit == 3)
  }

  it should "fail at binding, naming every reference no state message carried, and write nothing for them" in {
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
  }

  it should "fail at binding before any state message, naming every reference" in {
    val exec = build(Map("/limit" -> "n", "/name" -> "i"))
    assert(
      intercept[IllegalStateException](exec.bindStateReferences()).getMessage ==
        s"${missing("/limit", "n")}; ${missing("/name", "i")}"
    )
  }

  it should "keep failing until a message carries what was missing, then bind" in {
    val exec = build(Map("/limit" -> "n"))
    Seq(1, 2).foreach { _ =>
      assert(
        intercept[IllegalStateException](exec.bindStateReferences()).getMessage ==
          missing("/limit", "n")
      )
    }
    exec.registerState(State(Map("n" -> 4)))
    exec.bindStateReferences()
    assert(exec.setting.limit == 4)
  }

  it should "stop writing once bound: a later message is still registered, but the setting keeps its values" in {
    // Workers are recreated for each iteration, so a bound setting never needs rebinding.
    val exec = build(Map("/limit" -> "n", "/name" -> "i"))
    exec.registerState(State(Map("n" -> 2L, "i" -> "x")))
    exec.bindStateReferences()
    val later = State(Map("n" -> 9L, "i" -> "y"))
    exec.registerState(later)
    exec.bindStateReferences()
    assert(exec.state.contains(later))
    assert((exec.setting.limit, exec.setting.name) == ((2, "x")))
  }

  it should "reject a sidecar pointer that names no value of the setting when the executor is built" in {
    Seq("/nothing", "/tags/5", "/predicates", "/predicates/1", "/stateReferences/x", "").foreach {
      pointer =>
        val message = intercept[IllegalStateException](build(Map(pointer -> "k"))).getMessage
        assert(
          message == s"property $pointer refers to loop variable k, but ${classOf[SettingExec].getName} " +
            "parsed a setting with no value there",
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

  it should "bind an integer placeholder from an int, a long, a whole double or a numeric string" in {
    Seq[Any](7, 7L, 7.0, "7", " 7 ").foreach { value =>
      assert(bindOne("/limit", value).limit == 7, value)
    }
    Seq[Any](7.5, "seven", true, "").foreach { value =>
      assert(
        bindOneFails("/limit", value) ==
          s"property /limit refers to loop variable k, but its value $value is not an integer"
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

  it should "bind a number placeholder from any number or a numeric string" in {
    Seq[(Any, Double)](3 -> 3.0, 2.5 -> 2.5, "1.5" -> 1.5, 7L -> 7.0).foreach {
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
    Seq[(Any, String)](5L -> "5", 2.5 -> "2.5", true -> "true", "text" -> "text", "" -> "")
      .foreach {
        case (value, expected) => assert(bindOne("/name", value).name == expected)
      }
    assert(bindOne("/tags/1", "ünïcödé").tags == List("a", "ünïcödé"))
    // A Python None arrives as null; a list, a map or bytes has no text to bind.
    Seq[Any](null, List("a", "b"), Map("a" -> 1), Array[Byte](1, 2)).foreach { value =>
      assert(bindOneFails("/name", value).endsWith(s"its value $value is not a scalar"))
    }
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
