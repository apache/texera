/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.texera.common.compiler

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.common.compiler.MacroTestUtils._
import org.apache.texera.amber.util.JSONUtils.objectMapper

object MacroExpansionCases {
  private val output = "output-0"
  private val legacy = objectAt(
    objectMapper.readTree(getClass.getResource("/macros/nested-expansion.json"))
  )
  private val content = objectAt(legacy, "/content")
  private def unpack(body: ObjectNode): List[ObjectNode] = {
    val definition = objectAt(body, "/macroDependencies/0").deepCopy[ObjectNode]()
    val child = objectAt(definition, "/content")
    val nested = if (child.has("macroDependencies")) unpack(child) else Nil
    child.remove("macroDependencies")
    definition :: nested
  }
  private val pins = unpack(content)
  private val List(outer, _, inner) = pins
  private val one = objectAt(content, "/operators/0")
  private val two = objectAt(content, "/operators/1")
  private val unicode = one.at("/operatorProperties/value").asText()
  private val nested = changed(content, "macroDependencies" -> pins)
  private val expected = objectAt(legacy, "/expected")

  private def snapshot(
      content: ObjectNode,
      paths: Map[String, List[String]],
      ports: (String, List[ObjectNode])*
  ): ObjectNode =
    json("content" -> content, "paths" -> paths, "ports" -> json(ports: _*))

  private def nestedSnapshot(
      value: Any,
      includeTwo: Boolean = true,
      includeLeaves: Boolean = true
  ): ObjectNode = {
    val result = expected.deepCopy[ObjectNode]()
    val operators = arrayAt(result, "/content/operators")
    objectAt(operators.get(0), "/operatorProperties")
      .set[JsonNode]("value", objectMapper.valueToTree(value))
    for (i <- (operators.size() - 1 to 0 by -1) if !includeLeaves || (!includeTwo && i == 1)) {
      val id = operators.remove(i).path("operatorID").asText()
      val instance = objectAt(result, "/paths").remove(id).get(0).asText()
      val key = s"""["$instance","outputs","$output"]"""
      if (includeLeaves) objectAt(result, "/ports").remove(key)
      else arrayAt(result, s"/ports/$key").removeAll()
    }
    result
  }

  private def filter(id: String): ObjectNode = operator(id, "Filter", List("in"), List("out"))
  private val outputs = List("a", "b")
  private def fanoutBody(id: String): ObjectNode = {
    val inputs = outputs.zipWithIndex.map {
      case (leaf, i) =>
        link(s"${id}__0_in_$i", end(s"${id}__inputs", "in"), end(leaf, "in"))
    }
    val outgoing = outputs.map(leaf =>
      link(s"${id}__1_${leaf}_0", end(leaf, "out"), end(s"${id}__outputs", leaf))
    )
    macroGraph(id, outputs.map(filter), inputs ++ outgoing, List("in"), outputs)
  }

  private def fanoutInstance(
      id: String,
      macroID: String,
      name: String,
      version: Int = 1
  ): ObjectNode =
    instance(id, macroID, List("in"), outputs, version).put("customDisplayName", name)

  private val incoming = link("incoming", end("source", "out"), end("m", "in"))
  private val outgoing = link("outgoing", end("m", "b"), end("sink", "in"))
  private val fanout = pinned(
    graph(
      List(filter("source"), fanoutInstance("m", "fanout", "Fanout"), filter("sink")),
      List(incoming, outgoing)
    ),
    definition("fanout", "Fanout", fanoutBody("fanout"))
  )
  private val fanoutA = "da88d5c7-b748-5512-9a56-20a153a7d515"
  private val fanoutB = "b43e157f-d0f5-5383-9584-b62c40558563"
  private def fanoutPorts(id: String, a: String, b: String): List[(String, List[ObjectNode])] =
    List(
      s"""["$id","inputs","in"]""" -> List(end(a, "in"), end(b, "in")),
      s"""["$id","outputs","a"]""" -> List(end(a, "out")),
      s"""["$id","outputs","b"]""" -> List(end(b, "out"))
    )
  private val fanoutExpected = snapshot(
    graph(
      List("source", fanoutA, fanoutB, "sink").map(filter),
      List(
        link("653afb35-c8c5-530c-8cf4-07ab881c6ab5", end(fanoutB, "out"), end("sink", "in")),
        link("74334672-93bf-5467-aebe-0953ab46813b", end("source", "out"), end(fanoutA, "in")),
        link("17318951-ffc5-591e-85f8-8f8772ac33df", end("source", "out"), end(fanoutB, "in"))
      )
    ),
    Map(
      "source" -> List("source"),
      fanoutA -> List("m", "a"),
      fanoutB -> List("m", "b"),
      "sink" -> List("sink")
    ),
    fanoutPorts("m", fanoutA, fanoutB): _*
  )

  private val oldSelf = forwardingBody(
    "self",
    fanoutInstance("old-self", "self", "Self"),
    List("in" -> "in"),
    outputs.map(port => port -> port)
  )
  private val self = pinned(
    graph(List(fanoutInstance("self", "self", "Self", version = 4))),
    definition("self", "Self", fanoutBody("self")),
    definition("self", "Self", oldSelf, version = 4)
  )
  private val selfA = "dd8f47a9-6940-55f2-859f-b840f0c18dad"
  private val selfB = "be1a66b4-093c-5a81-9f6b-14059540081b"
  private val selfExpected = snapshot(
    graph(List(filter(selfA), filter(selfB))),
    Map(selfA -> List("self", "old-self", "a"), selfB -> List("self", "old-self", "b")),
    fanoutPorts("self", selfA, selfB): _*
  )

  private def positive(name: String, content: ObjectNode, expected: ObjectNode): ObjectNode =
    json("name" -> name, "content" -> content, "expected" -> expected)
  private def negative(name: String, content: ObjectNode, error: String): ObjectNode =
    json("name" -> name, "content" -> content, "error" -> error)
  private def editOne(fields: (String, Any)*): ObjectNode =
    changed(nested, "operators" -> List(changed(one, fields: _*), two))
  private def editPin(pin: ObjectNode)(edit: ObjectNode => Unit): ObjectNode = {
    val copy = pin.deepCopy[ObjectNode]()
    edit(objectAt(copy, "/content"))
    changed(nested, "macroDependencies" -> pins.map(p => if (p eq pin) copy else p))
  }

  val cases: List[ObjectNode] = List(
    legacy,
    positive("flat bundle", nested, expected),
    positive("portable bundle", nested, expected),
    positive(
      "disabled instance",
      changed(nested, "operators" -> List(one, changed(two, "isDisabled" -> true))),
      nestedSnapshot(unicode, includeTwo = false)
    ),
    positive(
      "disabled leaf",
      editPin(inner)(body => objectAt(body, "/operators/0").put("isDisabled", true)),
      nestedSnapshot(unicode, includeLeaves = false)
    ),
    positive(
      "null override",
      editOne("operatorProperties" -> json("value" -> null)),
      nestedSnapshot(null)
    ),
    positive(
      "empty override",
      editOne("operatorProperties" -> json("value" -> "")),
      nestedSnapshot("")
    ),
    negative(
      "missing definition",
      changed(nested, "macroDependencies" -> Nil),
      "Missing macro version: WorkflowMacro:outer:1"
    ),
    negative(
      "pin mismatch",
      editOne("operatorVersion" -> "2"),
      "Macro version does not match its pinned definition"
    ),
    negative(
      "unknown property",
      editOne("operatorProperties" -> json("value" -> unicode, "unknown" -> 1)),
      "Unknown macro property: unknown"
    ),
    negative(
      "duplicate operator ID",
      changed(nested, "operators" -> List(one, two, one)),
      "Duplicate graph element ID 'one'"
    ),
    negative(
      "duplicate port ID", {
        val content = nested.deepCopy[ObjectNode]()
        val operators = List(content.at("/operators/0"), content.at("/operators/1")) ++
          pins.indices.map(index => content.at(s"/macroDependencies/$index/content/operators/0"))
        operators.foreach(op => arrayAt(op, "/outputPorts").add(json("portID" -> output)))
        content
      },
      "Duplicate or invalid operator port"
    ),
    negative(
      "interface mismatch",
      editOne("inputPorts" -> List(json("portID" -> "unexpected"))),
      "Macro instance interface differs from its pinned version"
    ),
    negative(
      "missing endpoint",
      changed(
        nested,
        "links" -> List(link("missing", end("missing", "out"), end("one", "input-0")))
      ),
      "Missing operator or port in macro graph"
    ),
    negative(
      "conflicting definition",
      changed(
        nested,
        "macroDependencies" -> (pins :+ changed(
          outer,
          "content" ->
            changed(
              outer.path("content"),
              "links" -> List(json("linkID" -> "changed"), outer.at("/content/links/0"))
            )
        ))
      ),
      "Conflicting macro version"
    ),
    negative(
      "version cycle",
      editPin(outer)(body =>
        objectAt(body, "/operators/0").put("operatorType", "WorkflowMacro:outer:1")
      ),
      "Recursive macro reference"
    ),
    negative(
      "unconnected boundary",
      editPin(inner)(_.putArray("links")),
      "Connect or remove macro output 'output-0' before running."
    ),
    negative(
      "missing boundary target",
      editPin(inner)(body => objectAt(body, "/links/0/source").put("portID", "missing")),
      "Missing operator or port in macro graph"
    ),
    positive("input fanout and multiple outputs", fanout, fanoutExpected),
    negative(
      "duplicate link ID",
      changed(fanout, "links" -> List(incoming, outgoing, incoming)),
      "Duplicate graph element ID 'incoming'"
    ),
    negative(
      "expanded graph cycle",
      graph(
        List(filter("a"), filter("b")),
        List(
          link("ab", end("a", "out"), end("b", "in")),
          link("ba", end("b", "out"), end("a", "in"))
        )
      ),
      "Workflow contains a cycle"
    ),
    positive("finite older self revision", self, selfExpected)
  )
}
