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
import org.apache.texera.amber.core.workflow.UnknownPartition
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PortDescriptionSpec extends AnyFlatSpec with Matchers {

  private def desc(deps: List[Int] = List.empty): PortDescription =
    PortDescription(
      "p0",
      "input-0",
      disallowMultiInputs = true,
      isDynamicPort = false,
      UnknownPartition(),
      deps
    )

  "PortDescription" should "expose all constructor fields" in {
    val d = PortDescription(
      "p1",
      "out",
      disallowMultiInputs = false,
      isDynamicPort = true,
      UnknownPartition()
    )
    d.portID shouldBe "p1"
    d.displayName shouldBe "out"
    d.disallowMultiInputs shouldBe false
    d.isDynamicPort shouldBe true
    d.partitionRequirement shouldBe UnknownPartition()
  }

  it should "default dependencies to an empty list" in {
    desc().dependencies shouldBe empty
  }

  it should "accept explicit dependencies" in {
    desc(List(1, 2, 3)).dependencies shouldBe List(1, 2, 3)
  }

  it should "be value-equal and support copy" in {
    desc() shouldBe desc()
    desc().copy(displayName = "changed") should not be desc()
  }

  it should "ignore the legacy 'allowMultiInputs' JSON key for backward compat" in {
    val ann = classOf[PortDescription].getAnnotation(classOf[JsonIgnoreProperties])
    ann should not be null
    ann.value should contain("allowMultiInputs")
  }
}
