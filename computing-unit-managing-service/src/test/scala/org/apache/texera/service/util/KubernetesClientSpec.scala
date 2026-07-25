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

import io.fabric8.kubernetes.api.model.metrics.v1beta1.{
  ContainerMetricsBuilder,
  PodMetrics,
  PodMetricsBuilder
}
import io.fabric8.kubernetes.api.model.{Pod, PodBuilder, Quantity}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

// Exercises the pure fabric8 -> map transforms with builder-constructed model objects, so no
// cluster or client stubbing is needed. The status/metrics *decision* logic that consumes these
// maps (Running vs Pending, cpu/memory resolution) is covered by ComputingUnitHelpersSpec.
class KubernetesClientSpec extends AnyFlatSpec with Matchers {

  private def pod(cuid: Int, phase: String): Pod =
    new PodBuilder()
      .withNewMetadata()
      .withName(KubernetesClient.generatePodName(cuid))
      .endMetadata()
      .withNewStatus()
      .withPhase(phase)
      .endStatus()
      .build()

  // A pod whose status has not been populated yet (getStatus == null).
  private def statuslessPod(cuid: Int): Pod =
    new PodBuilder()
      .withNewMetadata()
      .withName(KubernetesClient.generatePodName(cuid))
      .endMetadata()
      .build()

  private def podMetrics(cuid: Int, cpu: String, memory: String): PodMetrics =
    new PodMetricsBuilder()
      .withNewMetadata()
      .withName(KubernetesClient.generatePodName(cuid))
      .endMetadata()
      .addToContainers(
        new ContainerMetricsBuilder()
          .withName("main")
          .withUsage(Map("cpu" -> new Quantity(cpu), "memory" -> new Quantity(memory)).asJava)
          .build()
      )
      .build()

  "generatePodName" should "prefix the cuid with computing-unit" in {
    KubernetesClient.generatePodName(42) shouldBe "computing-unit-42"
  }

  it should "handle a cuid of 0" in {
    KubernetesClient.generatePodName(0) shouldBe "computing-unit-0"
  }

  "phasesByPodName" should "map every pod name to its phase" in {
    val phases = KubernetesClient.phasesByPodName(Seq(pod(1, "Running"), pod(2, "Pending")))
    phases(KubernetesClient.generatePodName(1)) shouldBe "Running"
    phases(KubernetesClient.generatePodName(2)) shouldBe "Pending"
  }

  it should "map a pod with no status to a null phase but still include it" in {
    val phases = KubernetesClient.phasesByPodName(Seq(statuslessPod(3)))
    phases should contain key KubernetesClient.generatePodName(3)
    phases(KubernetesClient.generatePodName(3)) shouldBe null
  }

  "metricsByPodName" should "flatten each pod's container usage into a cpu/memory map" in {
    val metrics =
      KubernetesClient.metricsByPodName(Seq(podMetrics(1, "250m", "128Mi")))
    metrics(KubernetesClient.generatePodName(1)) shouldBe Map("cpu" -> "250m", "memory" -> "128Mi")
  }
}
