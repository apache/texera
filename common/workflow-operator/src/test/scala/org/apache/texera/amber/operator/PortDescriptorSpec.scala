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

import org.apache.texera.amber.core.workflow.UnknownPartition
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PortDescriptorSpec extends AnyFlatSpec with Matchers {

  private def portDesc(id: String, name: String): PortDescription =
    PortDescription(
      id,
      name,
      disallowMultiInputs = false,
      isDynamicPort = false,
      UnknownPartition()
    )

  "PortDescriptor" should "default inputPorts and outputPorts to null (not an empty list)" in {
    val pd = new PortDescriptor {}
    pd.inputPorts shouldBe null
    pd.outputPorts shouldBe null
  }

  it should "allow inputPorts and outputPorts to be reassigned" in {
    val pd = new PortDescriptor {}
    val in = List(portDesc("i", "in"))
    val out = List(portDesc("o", "out"))
    pd.inputPorts = in
    pd.outputPorts = out
    pd.inputPorts shouldBe in
    pd.outputPorts shouldBe out
  }
}
