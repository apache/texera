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

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonProperty, JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.exc.InvalidFormatException
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.texera.amber.core.state.StateReferencing.{
  literalReferences,
  referencedVariable,
  textReferences
}
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

/**
  * `StateReferenceModule` puts a typed placeholder where a `$name` loop-variable reference cannot
  * be converted to its property's type, and records the property's pointer relative to the
  * `StateReferencing` object. The beans below are parsed through `JSONUtils.objectMapper`, where
  * the module is registered: directly, polymorphically with the type id first, in the middle and
  * last (Jackson hands the object over differently in each case), and nested in other documents.
  */
class StateReferenceModuleSpec extends AnyFlatSpec {

  import StateReferenceModuleSpec._

  private def parse(json: String): Bean = objectMapper.readValue(json, classOf[Bean])

  private def typed(json: String): TypedBean =
    objectMapper.readValue(json, classOf[Base]).asInstanceOf[TypedBean]

  private val plainMapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  "StateReferenceModule" should "put 0 / 0.0 / false where a typed property holds '$name', and record its pointer" in {
    val bean = parse(
      """{"int":"$a","long":"$b","double":"$c","bool":"$d","boxed":"$e","opt":"$f","float":"$g"}"""
    )
    assert(bean.int == 0)
    assert(bean.long == 0L)
    assert(bean.double == 0.0)
    assert(!bean.bool)
    assert(bean.boxed == Integer.valueOf(0))
    assert(bean.opt.contains(0))
    assert(bean.float == 0f)
    assert(
      bean.stateReferences == Map(
        "/int" -> "a",
        "/long" -> "b",
        "/double" -> "c",
        "/bool" -> "d",
        "/boxed" -> "e",
        "/opt" -> "f",
        "/float" -> "g"
      )
    )
  }

  it should "record a reference inside a list of beans under its full pointer" in {
    val bean = parse(
      """{"nested":[{"label":"x","threshold":1.5},{"label":"$l","threshold":"$t","count":"$n"}]}"""
    )
    assert(bean.nested.map(_.threshold) == List(1.5, 0.0))
    assert(bean.nested.map(_.count) == List(0, 0))
    assert(bean.nested(1).label == "$l")
    assert(bean.stateReferences == Map("/nested/1/threshold" -> "t", "/nested/1/count" -> "n"))
  }

  it should "keep '$s' in a String property and record nothing: the compiler finds the strings" in {
    val bean = parse("""{"name":"$s","items":["$t"],"int":"$a"}""")
    assert(bean.name == "$s")
    assert(bean.items == List("$t"))
    assert(bean.stateReferences == Map("/int" -> "a"))
  }

  it should "leave every value that is not a whole '$name' to Jackson" in {
    val bean = parse("""{"name":"cost is $5","items":["$1","$","a$b"]}""")
    assert(bean.name == "cost is $5")
    assert(bean.items == List("$1", "$", "a$b"))
    assert(bean.stateReferences.isEmpty)
    // In a typed property they fail as they always did; so does a reference padded with blanks,
    // which Jackson trims before converting but which is not a reference as a string either.
    Seq("$1", "$", "cost is $5", "$a b", " $a", "$a ").foreach { value =>
      assertThrows[InvalidFormatException](parse(s"""{"int":"$value"}"""))
    }
  }

  it should "let '$c' into an enum property fail with Jackson's ordinary error" in {
    val ex = intercept[InvalidFormatException](parse("""{"color":"$c"}"""))
    assert(ex.getMessage.contains("not one of the values accepted"))
  }

  it should "not touch a '$n' outside every StateReferencing object" in {
    assertThrows[InvalidFormatException](
      objectMapper.readValue("""{"count":"$n"}""", classOf[Wrapper])
    )
  }

  it should "parse a reference-free object exactly as a mapper without the module does" in {
    Seq(
      """{"name":"n","int":3,"double":0.5,"bool":true,"items":["a"],"color":"integer"}""",
      """{"name":1.10,"int":3,"nested":[{"label":"x","threshold":2}],"stateReferences":{}}"""
    ).foreach { json =>
      val withModule = parse(json)
      assert(withModule.stateReferences.isEmpty)
      assert(
        objectMapper.writeValueAsString(withModule) ==
          objectMapper.writeValueAsString(plainMapper.readValue(json, classOf[Bean])),
        json
      )
    }
    val json = """{"items":["a"],"type":"typed","limit":3,"name":1.10}"""
    assert(
      objectMapper.writeValueAsString(typed(json)) ==
        objectMapper.writeValueAsString(plainMapper.readValue(json, classOf[Base]))
    )
  }

  it should "set the sidecar to what the parse recorded, whatever the JSON carried" in {
    assert(parse("""{"int":0,"stateReferences":{"/int":"a"}}""").stateReferences.isEmpty)
    assert(
      parse("""{"int":"$b","stateReferences":{"/int":"a"}}""").stateReferences == Map("/int" -> "b")
    )
  }

  // ---------------------------------------------------------------------------
  // How the object is handed over: the pointer is relative to it every time
  // ---------------------------------------------------------------------------

  it should "record the same pointers whether the type id comes first, in the middle or last" in {
    // In the middle, the frontend's order: its properties, then operatorType, then the ports.
    // Jackson replays the fields before the type id from a buffer, here starting with a list.
    Seq(
      """{"type":"typed","items":["$x"],"limit":"$i","ratio":"$r"}""",
      """{"items":["$x"],"limit":"$i","type":"typed","ratio":"$r"}""",
      """{"limit":"$i","items":["$x"],"ratio":"$r","type":"typed"}""",
      """{"items":["$x"],"limit":"$i","ratio":"$r","type":"typed"}"""
    ).foreach { json =>
      val bean = typed(json)
      assert(bean.limit == 0, json)
      assert(bean.ratio == 0.0, json)
      assert(bean.items == List("$x"), json)
      assert(bean.stateReferences == Map("/limit" -> "i", "/ratio" -> "r"), json)
    }
  }

  it should "record the pointer when the concrete subtype is requested directly" in {
    Seq("""{"type":"typed","limit":"$i"}""", """{"items":[],"limit":"$i","type":"typed"}""")
      .foreach { json =>
        val bean = objectMapper.readValue(json, classOf[TypedBean])
        assert(bean.stateReferences == Map("/limit" -> "i"), json)
      }
  }

  it should "parse an object whose only field is the type id" in {
    assert(typed("""{"type":"typed"}""").stateReferences.isEmpty)
  }

  it should "record a pointer relative to a bean nested in another document" in {
    val wrapper = objectMapper.readValue(
      """{"note":"n","wrapped":{"name":"x","nested":[{"count":"$n"}]}}""",
      classOf[Wrapper]
    )
    assert(wrapper.wrapped.stateReferences == Map("/nested/0/count" -> "n"))
  }

  it should "record pointers relative to each operator of a whole plan, even one Jackson buffered" in {
    // As the websocket request carries a plan: the operators sit at /plan/operators/i, and the
    // request's own type id comes last, so Jackson replays the whole plan from a buffer.
    val request = objectMapper.readValue(
      """{"plan":{"operators":[{"type":"typed","limit":1},
        |{"items":["a"],"limit":"$i","type":"typed","ratio":"$r"}]},"kind":"envelope"}""".stripMargin,
      classOf[Request]
    )
    val operators = request.asInstanceOf[Envelope].plan.operators.map(_.asInstanceOf[TypedBean])
    assert(operators.map(_.limit) == List(1, 0))
    assert(operators.head.stateReferences.isEmpty)
    assert(operators(1).stateReferences == Map("/limit" -> "i", "/ratio" -> "r"))
  }

  // ---------------------------------------------------------------------------
  // The helpers the compiler and the worker share
  // ---------------------------------------------------------------------------

  "StateReferencing.referencedVariable" should "match only a whole '$name' string" in {
    assert(referencedVariable("$K").contains("K"))
    assert(referencedVariable("$_ok").contains("_ok"))
    assert(referencedVariable("$a1_B2").contains("a1_B2"))
    Seq("cost is $5", "$1", "a$b", "$", "", "K", "$K ", " $K", "$$K", "$K-1", "$K.x", "$K\n")
      .foreach(value => assert(referencedVariable(value).isEmpty, s"'$value' is not a reference"))
  }

  "StateReferencing.literalReferences" should "find every whole-string '$name' outside the sidecar, escaping pointer segments" in {
    val tree = objectMapper
      .readTree(
        """{"name":"$i","limit":0,"tags":["a","$t"],"a/b~c":{"deep":[{"v":"$z"}]},
          |"note":"cost is $5","stateReferences":{"/name":"$j"}}""".stripMargin
      )
      .asInstanceOf[ObjectNode]
    assert(
      literalReferences(tree) ==
        Map("/name" -> "i", "/tags/1" -> "t", "/a~1b~0c/deep/0/v" -> "z")
    )
  }

  "StateReferencing.textReferences" should "keep the references whose value is the '$name' text itself" in {
    val tree = objectMapper
      .readTree(
        """{"name":"$i","limit":0,"ratio":0.0,"flag":false,"tags":["a","$t"],"other":"$j",
          |"note":"rbf","count":"0"}""".stripMargin
      )
      .asInstanceOf[ObjectNode]
    val references = Map(
      "/name" -> "i",
      "/tags/1" -> "t",
      // Typed placeholders: the parse put a value of the property's own type there.
      "/limit" -> "n",
      "/ratio" -> "r",
      "/flag" -> "f",
      // Text, but not this reference's text: another name, a plain word, a placeholder-looking "0".
      "/other" -> "i",
      "/note" -> "k",
      "/count" -> "c",
      // Nothing there at all.
      "/missing" -> "m",
      "/tags/5" -> "t"
    )
    assert(textReferences(tree, references) == Map("/name" -> "i", "/tags/1" -> "t"))
    assert(textReferences(tree, Map.empty).isEmpty)
    assert(textReferences(objectMapper.createObjectNode(), references).isEmpty)
  }
}

object StateReferenceModuleSpec {

  class Nested {
    @JsonProperty var label: String = _
    @JsonProperty var threshold: Double = _
    @JsonProperty var count: Int = _
  }

  /** One property of each scalar type, a boxed Integer, an Option, lists and an enum. */
  class Bean extends StateReferencing {
    @JsonProperty var name: String = _
    @JsonProperty var int: Int = _
    @JsonProperty var long: Long = _
    @JsonProperty var double: Double = _
    @JsonProperty var float: Float = _
    @JsonProperty var bool: Boolean = _
    @JsonProperty var boxed: java.lang.Integer = _
    // The Option's value type is erased on the JVM: Jackson needs the hint to see an Int.
    @JsonProperty
    @JsonDeserialize(contentAs = classOf[java.lang.Integer])
    var opt: Option[Int] = None
    @JsonProperty var items: List[String] = List.empty
    @JsonProperty var nested: List[Nested] = List.empty
    @JsonProperty var color: AttributeType = _ // a Java enum
  }

  /** Not a StateReferencing object itself: it only holds one. */
  class Wrapper {
    @JsonProperty var note: String = _
    @JsonProperty var count: Int = _
    @JsonProperty var wrapped: Bean = _
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(Array(new Type(value = classOf[TypedBean], name = "typed")))
  abstract class Base extends StateReferencing

  class TypedBean extends Base {
    @JsonProperty var items: List[String] = List.empty
    @JsonProperty var limit: Int = _
    @JsonProperty var ratio: Double = _
    @JsonProperty var name: String = _
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "kind")
  @JsonSubTypes(Array(new Type(value = classOf[Envelope], name = "envelope")))
  trait Request

  case class Plan(operators: List[Base])

  case class Envelope(plan: Plan) extends Request
}
