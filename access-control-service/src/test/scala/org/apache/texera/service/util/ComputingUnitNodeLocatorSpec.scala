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

package org.apache.texera.service.util

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.texera.common.config.KubernetesConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

class ComputingUnitNodeLocatorSpec extends AnyFlatSpec with Matchers {

  private val mapper = new ObjectMapper()
  private def pod(json: String): JsonNode = mapper.readTree(json)

  private def locator(
      pods: Map[String, JsonNode],
      asked: mutable.Buffer[String] = mutable.Buffer()
  ): ComputingUnitNodeLocator =
    new ComputingUnitNodeLocator(name => { asked += name; pods.get(name) })

  private val podName = s"${KubernetesConfig.computeUnitPodNamePrefix}-7"

  "nodeIpOf" should "return the host IP of the computing unit's pod" in {
    locator(Map(podName -> pod("""{"status":{"hostIP":"10.0.0.4"}}"""))).nodeIpOf(7) shouldBe
      Some("10.0.0.4")
  }

  it should "ask for the pod named by the configured prefix and the cuid" in {
    val asked = mutable.Buffer[String]()
    locator(Map.empty, asked).nodeIpOf(7)
    asked should contain only podName
  }

  it should "return None when the computing unit has no pod" in {
    locator(Map.empty).nodeIpOf(7) shouldBe None
  }

  it should "return None while the pod is not scheduled yet" in {
    locator(Map(podName -> pod("""{"status":{"phase":"Pending"}}"""))).nodeIpOf(7) shouldBe None
    locator(Map(podName -> pod("""{"status":{"hostIP":""}}"""))).nodeIpOf(7) shouldBe None
  }

  it should "propagate a lookup failure rather than reporting the unit as unscheduled" in {
    val failing = new ComputingUnitNodeLocator(_ => throw new IllegalStateException("forbidden"))
    an[IllegalStateException] should be thrownBy failing.nodeIpOf(7)
  }
}
