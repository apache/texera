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

package org.apache.texera.amber.operator.visualization.urlviz

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import javax.validation.constraints.NotNull

class UrlVizOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  // ---------------------------------------------------------------------------
  // operatorInfo
  // ---------------------------------------------------------------------------

  "UrlVizOpDesc.operatorInfo" should
    "advertise the URL Visualizer name and Visualization-Media group" in {
    val info = (new UrlVizOpDesc).operatorInfo
    info.userFriendlyName shouldBe "URL Visualizer"
    info.operatorGroupName shouldBe OperatorGroupConstants.VISUALIZATION_MEDIA_GROUP
    info.operatorDescription.toLowerCase should include("url")
  }

  // ---------------------------------------------------------------------------
  // getPhysicalOp — wiring + output schema
  // ---------------------------------------------------------------------------

  "UrlVizOpDesc.getPhysicalOp" should
    "wire the UrlVizOpExec class name into the OpExecInitInfo" in {
    val physical = (new UrlVizOpDesc).getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.visualization.urlviz.UrlVizOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
  }

  it should "produce an output schema with a single `html-content` STRING attribute" in {
    val op = new UrlVizOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    // The propagation function ignores its input schemas — it always
    // emits the fixed `html-content: STRING` schema on the (single)
    // output port.
    val out = physical.propagateSchema.func(Map.empty)
    val outputId = op.operatorInfo.outputPorts.head.id
    out.keySet shouldBe Set(outputId)
    val schema: Schema = out(outputId)
    schema.getAttributes should have size 1
    val attr: Attribute = schema.getAttributes.head
    attr.getName shouldBe "html-content"
    attr.getType shouldBe AttributeType.STRING
  }

  // ---------------------------------------------------------------------------
  // Field annotations
  // ---------------------------------------------------------------------------

  "UrlVizOpDesc#urlContentAttrName" should
    "carry @JsonProperty(required = true)" in {
    val jp = classOf[UrlVizOpDesc]
      .getDeclaredField("urlContentAttrName")
      .getAnnotation(classOf[JsonProperty])
    jp should not be null
    jp.required shouldBe true
  }

  it should "carry @AutofillAttributeName (UI populates the attribute dropdown)" in {
    val ann = classOf[UrlVizOpDesc]
      .getDeclaredField("urlContentAttrName")
      .getAnnotation(classOf[AutofillAttributeName])
    ann should not be null
  }

  it should "carry @NotNull (javax.validation contract)" in {
    val notNull = classOf[UrlVizOpDesc]
      .getDeclaredField("urlContentAttrName")
      .getAnnotation(classOf[NotNull])
    notNull should not be null
  }

  "UrlVizOpDesc (class-level)" should
    "carry @JsonSchemaInject restricting `urlContentAttrName` to STRING attributes" in {
    val ann = classOf[UrlVizOpDesc].getAnnotation(classOf[JsonSchemaInject])
    ann should not be null
    val payload = ann.json
    payload should include("attributeTypeRules")
    payload should include("urlContentAttrName")
    payload should include("string")
  }

  // ---------------------------------------------------------------------------
  // Independent instances
  // ---------------------------------------------------------------------------

  "UrlVizOpDesc" should
    "assign a fresh operatorIdentifier per instance (UUID-based id is not shared)" in {
    val a = new UrlVizOpDesc
    val b = new UrlVizOpDesc
    a.operatorIdentifier should not equal b.operatorIdentifier
  }
}
