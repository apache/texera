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

package org.apache.texera.amber.operator.visualization.htmlviz

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.executor.ExecFactory
import org.apache.texera.amber.core.state.{State, StateReferencing}
import org.apache.texera.amber.core.tuple._
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class HtmlVizOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val schema = new Schema(
    new Attribute("field1", AttributeType.STRING),
    new Attribute("field2", AttributeType.STRING)
  )
  val opDesc: HtmlVizOpDesc = new HtmlVizOpDesc()

  val outputSchema: Schema =
    opDesc.getExternalOutputSchemas(Map(PortIdentity() -> schema)).values.head

  def tuple(): Tuple =
    Tuple
      .builder(schema)
      .addSequentially(Array("hello", "<html></html>"))
      .build()

  it should "process a target field" in {
    opDesc.htmlContentAttrName = "field1"
    val htmlVizOpExec = new HtmlVizOpExec(objectMapper.writeValueAsString(opDesc))
    htmlVizOpExec.open()
    val processedTuple: Tuple =
      htmlVizOpExec
        .processTuple(tuple(), 0)
        .next()
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)

    assert(processedTuple.getField("html-content").asInstanceOf[String] == "hello")

  }

  it should "process another target field" in {
    opDesc.htmlContentAttrName = "field2"
    val htmlVizOpExec = new HtmlVizOpExec(objectMapper.writeValueAsString(opDesc))
    htmlVizOpExec.open()
    val processedTuple: Tuple =
      htmlVizOpExec
        .processTuple(tuple(), 0)
        .next()
        .asInstanceOf[SchemaEnforceable]
        .enforceSchema(outputSchema)

    assert(processedTuple.getField("html-content").asInstanceOf[String] == "<html></html>")

  }

  it should "throw an AssertionError (not a NullPointerException) on open() when html content is left empty" in {
    val emptyDesc = new HtmlVizOpDesc()
    // htmlContentAttrName left at its "" default
    val htmlVizOpExec = new HtmlVizOpExec(objectMapper.writeValueAsString(emptyDesc))
    val ex = intercept[AssertionError](htmlVizOpExec.open())
    assert(ex.getMessage != null)
    assert(ex.getMessage.contains("HTML content cannot be empty"))
  }

  it should "open() successfully when html content is configured" in {
    val configuredDesc = new HtmlVizOpDesc()
    configuredDesc.htmlContentAttrName = "field1"
    val htmlVizOpExec = new HtmlVizOpExec(objectMapper.writeValueAsString(configuredDesc))
    htmlVizOpExec.open()
  }

  // ---------------------------------------------------------------------------
  // Inside a loop block: the setting is written after open()
  // ---------------------------------------------------------------------------

  /** Opened as the worker opens it, with the html content referring to `h`, then `h` written in. */
  private def openedInsideLoopBlock(h: String): HtmlVizOpExec = {
    val desc = new HtmlVizOpDesc()
    desc.htmlContentAttrName = "$h"
    val node = objectMapper.valueToTree[ObjectNode](desc)
    node.putObject(StateReferencing.SIDECAR_PROPERTY).put("/htmlContentAttrName", "h")
    val exec = ExecFactory
      .newExecFromJavaClassName(
        classOf[HtmlVizOpExec].getName,
        objectMapper.writeValueAsString(node)
      )
      .asInstanceOf[HtmlVizOpExec]
    exec.open()
    exec.registerState(State(Map("h" -> h)))
    exec.bindStateReferences()
    exec
  }

  "HtmlVizOpExec inside a loop block" should "render the field the loop variable written after open() names" in {
    val processedTuple = openedInsideLoopBlock("field2")
      .processTuple(tuple(), 0)
      .next()
      .asInstanceOf[SchemaEnforceable]
      .enforceSchema(outputSchema)
    assert(processedTuple.getField("html-content").asInstanceOf[String] == "<html></html>")
  }

  it should "reject an empty html content written after open() at the first tuple, as open() does" in {
    // open() saw the placeholder "$h"; the empty value arrives with the loop state.
    val exec = openedInsideLoopBlock("")
    val ex = intercept[AssertionError](exec.processTuple(tuple(), 0))
    assert(ex.getMessage.contains("HTML content cannot be empty"))
  }
}
