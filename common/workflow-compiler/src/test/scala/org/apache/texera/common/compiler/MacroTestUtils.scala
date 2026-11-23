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
package org.apache.texera.common.compiler

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.common.compiler.model.LogicalPlanPojo

/** Typed access to mutable fixture nodes; pointers stay explicit at each mutation. */
object MacroTestUtils {
  def objectAt(node: JsonNode, pointer: String = ""): ObjectNode =
    node.at(pointer).asInstanceOf[ObjectNode]

  def arrayAt(node: JsonNode, pointer: String): ArrayNode =
    node.at(pointer).asInstanceOf[ArrayNode]

  def json(fields: (String, Any)*): ObjectNode = {
    val node = objectMapper.createObjectNode()
    fields.foreach { case (key, value) => node.set[JsonNode](key, objectMapper.valueToTree(value)) }
    node
  }

  def changed(node: JsonNode, fields: (String, Any)*): ObjectNode =
    node.deepCopy[ObjectNode]().setAll[ObjectNode](json(fields: _*))

  def operator(
      id: String,
      kind: String,
      inputs: List[String] = Nil,
      outputs: List[String] = Nil,
      properties: JsonNode = json(),
      version: Long = 1
  ): ObjectNode =
    json(
      "operatorID" -> id,
      "operatorType" -> kind,
      "operatorVersion" -> version.toString,
      "operatorProperties" -> properties,
      "inputPorts" -> inputs.map(id => json("portID" -> id)),
      "outputPorts" -> outputs.map(id => json("portID" -> id)),
      "showAdvanced" -> false
    )

  def terminal(id: String, input: Boolean, ports: List[String]): ObjectNode =
    changed(
      if (input) operator(id, "MacroInput", outputs = ports)
      else operator(id, "MacroOutput", inputs = ports),
      "dynamicInputPorts" -> !input,
      "dynamicOutputPorts" -> input
    )

  def textSource(id: String, text: String): ObjectNode =
    operator(
      id,
      "TextInput",
      outputs = List("output-0"),
      properties = json("textInput" -> text, "attributeName" -> "line", "attributeType" -> "string")
    )

  def field(id: String, operatorID: String, property: String): ObjectNode =
    json("id" -> id, "operatorID" -> operatorID, "propertyKey" -> property)

  def formBinding(fields: ObjectNode*): ObjectNode = json("fields" -> fields)

  def end(id: String, port: String): ObjectNode = json("operatorID" -> id, "portID" -> port)

  def link(id: String, source: ObjectNode, target: ObjectNode): ObjectNode =
    json("linkID" -> id, "source" -> source, "target" -> target)

  def graph(operators: List[JsonNode], links: List[JsonNode] = Nil): ObjectNode =
    json("operators" -> operators, "links" -> links)

  def macroGraph(
      id: String,
      operators: List[JsonNode],
      links: List[JsonNode] = Nil,
      inputs: List[String] = Nil,
      outputs: List[String] = Nil
  ): ObjectNode = {
    val terminals = List(
      terminal(s"${id}__inputs", input = true, inputs),
      terminal(s"${id}__outputs", input = false, outputs)
    )
    changed(graph(operators ++ terminals, links), "macroInterface" -> json("id" -> id))
  }

  def pinned(graph: ObjectNode, definitions: ObjectNode*): ObjectNode =
    changed(graph, "macroDependencies" -> definitions)

  def macroPlan(content: JsonNode, results: List[String] = Nil): LogicalPlanPojo =
    LogicalPlanPojo(Nil, Nil, results, Nil, Some(content))

  def instance(
      id: String,
      macroID: String,
      inputs: List[String] = Nil,
      outputs: List[String] = Nil,
      version: Long = 1
  ): ObjectNode =
    operator(id, s"WorkflowMacro:$macroID:$version", inputs, outputs, version = version)

  /** Expose named ports of a single child through the saved terminal nodes. */
  def forwardingBody(
      id: String,
      child: ObjectNode,
      inputs: List[(String, String)],
      outputs: List[(String, String)]
  ): ObjectNode = {
    val childID = child.get("operatorID").asText()
    val incoming = inputs.map {
      case (publicPort, port) =>
        link(s"${id}__0_${publicPort}_0", end(s"${id}__inputs", publicPort), end(childID, port))
    }
    val outgoing = outputs.map {
      case (publicPort, port) =>
        link(s"${id}__1_${publicPort}_0", end(childID, port), end(s"${id}__outputs", publicPort))
    }
    macroGraph(id, List(child), incoming ++ outgoing, inputs.map(_._1), outputs.map(_._1))
  }

  def passthroughWorkflow: ObjectNode = {
    val inputs = List("rows")
    val outputs = List("out")
    val body = macroGraph(
      "identity",
      Nil,
      List(link("passthrough", end("identity__inputs", "rows"), end("identity__outputs", "out"))),
      inputs,
      outputs
    )
    objectAt(body, "/operators/0/outputPorts/0").put("displayName", "Rows 東京")
    pinned(
      graph(
        List(
          textSource("source", "3\n1"),
          instance("macro", "identity", inputs, outputs),
          operator("sink", "Limit", List("input-0"), List("output-0"), json("limit" -> 1))
        ),
        List(
          link("in", end("source", "output-0"), end("macro", "rows")),
          link("out", end("macro", "out"), end("sink", "input-0"))
        )
      ),
      definition("identity", "Identity", body)
    )
  }

  def definition(id: String, name: String, content: ObjectNode, version: Int = 1): ObjectNode =
    json("id" -> id, "version" -> version, "name" -> name, "content" -> content)
}
