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

package org.apache.texera.amber.operator.metadata

import org.apache.texera.amber.core.workflow.OutputPort.OutputMode
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OperatorInfoSpec extends AnyFlatSpec with Matchers {

  "OperatorInfo" should
    "expose its constructor fields and default the four boolean flags to false" in {
    val info = OperatorInfo("Op", "desc", "Group", List(InputPort()), List(OutputPort()))
    info.userFriendlyName shouldBe "Op"
    info.operatorDescription shouldBe "desc"
    info.operatorGroupName shouldBe "Group"
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.dynamicInputPorts shouldBe false
    info.dynamicOutputPorts shouldBe false
    info.supportReconfiguration shouldBe false
    info.allowPortCustomization shouldBe false
  }

  it should "carry the boolean flags when explicitly set" in {
    val info = OperatorInfo(
      "Op",
      "d",
      "G",
      List(InputPort()),
      List(OutputPort()),
      dynamicInputPorts = true,
      dynamicOutputPorts = true,
      supportReconfiguration = true,
      allowPortCustomization = true
    )
    info.dynamicInputPorts shouldBe true
    info.dynamicOutputPorts shouldBe true
    info.supportReconfiguration shouldBe true
    info.allowPortCustomization shouldBe true
  }

  "OperatorInfo.forVisualization" should
    "build a single disallow-multi-links input and a single SINGLE_SNAPSHOT output" in {
    val info = OperatorInfo.forVisualization("Viz", "render", "VizGroup")
    info.userFriendlyName shouldBe "Viz"
    info.operatorDescription shouldBe "render"
    info.operatorGroupName shouldBe "VizGroup"
    info.inputPorts shouldBe List(InputPort(disallowMultiLinks = true))
    info.outputPorts shouldBe List(OutputPort(mode = OutputMode.SINGLE_SNAPSHOT))
  }

  "OperatorInfo equality" should "be value-based (case class)" in {
    val a = OperatorInfo("Op", "d", "G", List(InputPort()), List(OutputPort()))
    val b = OperatorInfo("Op", "d", "G", List(InputPort()), List(OutputPort()))
    a shouldBe b
    a.copy(supportReconfiguration = true) should not be b
  }
}
