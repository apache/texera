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

package org.apache.texera.amber.operator

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.texera.amber.core.workflow.HashPartition
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class PortDescriptorSpec extends AnyFlatSpec {

  private class TestPortDescriptor extends PortDescriptor

  private val inputPort = PortDescription(
    portID = "input-0",
    displayName = "Input",
    disallowMultiInputs = true,
    isDynamicPort = false,
    partitionRequirement = HashPartition(List("city")),
    dependencies = List(1, 2)
  )

  private val outputPort = PortDescription(
    portID = "output-0",
    displayName = "Output",
    disallowMultiInputs = false,
    isDynamicPort = true,
    partitionRequirement = HashPartition(),
    dependencies = List.empty
  )

  "PortDescriptor" should "default inputPorts and outputPorts to null" in {
    val descriptor = new TestPortDescriptor

    assert(descriptor.inputPorts == null)
    assert(descriptor.outputPorts == null)
  }

  it should "allow inputPorts and outputPorts to be assigned after construction" in {
    val descriptor = new TestPortDescriptor

    descriptor.inputPorts = List(inputPort)
    descriptor.outputPorts = List(outputPort)

    assert(descriptor.inputPorts == List(inputPort))
    assert(descriptor.outputPorts == List(outputPort))
  }

  "PortDescription" should "preserve every constructor field" in {
    assert(inputPort.portID == "input-0")
    assert(inputPort.displayName == "Input")
    assert(inputPort.disallowMultiInputs)
    assert(!inputPort.isDynamicPort)
    assert(inputPort.partitionRequirement == HashPartition(List("city")))
    assert(inputPort.dependencies == List(1, 2))
  }

  it should "support case-class equality and copy semantics" in {
    val copied = inputPort.copy(displayName = "Renamed")

    assert(inputPort == inputPort.copy())
    assert(copied.portID == inputPort.portID)
    assert(copied.displayName == "Renamed")
    assert(copied != inputPort)
  }

  it should "carry the allowMultiInputs Jackson ignore-property shim" in {
    val annotation = classOf[PortDescription].getAnnotation(classOf[JsonIgnoreProperties])

    assert(annotation != null)
    assert(annotation.value().contains("allowMultiInputs"))
  }

  it should "round-trip through JSON while preserving every field" in {
    val json = objectMapper.writeValueAsString(inputPort)
    val restored = objectMapper.readValue(json, classOf[PortDescription])

    assert(restored == inputPort)
  }

  it should "ignore the legacy allowMultiInputs JSON field during deserialization" in {
    val json =
      """
        |{
        |  "portID": "input-0",
        |  "displayName": "Input",
        |  "disallowMultiInputs": true,
        |  "allowMultiInputs": false,
        |  "isDynamicPort": false,
        |  "partitionRequirement": {
        |    "type": "hash",
        |    "hashAttributeNames": ["city"]
        |  },
        |  "dependencies": [1, 2]
        |}
        |""".stripMargin

    assert(objectMapper.readValue(json, classOf[PortDescription]) == inputPort)
  }
}
