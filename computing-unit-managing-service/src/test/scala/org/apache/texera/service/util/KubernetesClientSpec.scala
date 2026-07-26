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
  PodMetricsBuilder,
  PodMetricsList,
  PodMetricsListBuilder
}
import io.fabric8.kubernetes.api.model.{Pod, PodBuilder, PodList, PodListBuilder, Quantity}
import io.fabric8.kubernetes.client.dsl.{
  MetricAPIGroupDSL,
  MixedOperation,
  NonNamespaceOperation,
  PodMetricOperation,
  PodResource
}
import io.fabric8.kubernetes.client.{KubernetesClient => Fabric8Client}
import org.apache.texera.common.config.KubernetesConfig
import org.mockito.Mockito.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

// Two layers are exercised here:
//   * the pure fabric8 -> map transforms (phasesByPodName / metricsByPodName) with
//     builder-constructed model objects, so the transform logic needs no client, and
//   * the thin namespace-wide wrappers (getAllPodPhases / getAllPodMetrics / getPodMetrics),
//     which are driven through a freshly constructed KubernetesClient whose fabric8 client is a
//     Mockito stub — no live cluster and no mutable global.
// The status/metrics *decision* logic that consumes these maps (Running vs Pending, cpu/memory
// resolution) is covered by ComputingUnitHelpersSpec.
class KubernetesClientSpec extends AnyFlatSpec with Matchers {

  private val namespace: String = KubernetesConfig.computeUnitPoolNamespace

  // A fabric8 client stubbed just enough to answer the namespace-wide pod-list and pod-metrics
  // calls the wrappers make. RETURNS_DEEP_STUBS can't be used: fabric8's fluent API returns type
  // variables, so each step of the chain is mocked explicitly.
  private def stubbedClient(pods: Seq[Pod], metrics: Seq[PodMetrics]): Fabric8Client = {
    val client = mock(classOf[Fabric8Client])

    val podsMixed = mock(classOf[MixedOperation[_, _, _]])
      .asInstanceOf[MixedOperation[Pod, PodList, PodResource]]
    val podsInNamespace = mock(classOf[NonNamespaceOperation[_, _, _]])
      .asInstanceOf[NonNamespaceOperation[Pod, PodList, PodResource]]
    when(client.pods()).thenReturn(podsMixed)
    when(podsMixed.inNamespace(namespace)).thenReturn(podsInNamespace)
    when(podsInNamespace.list()).thenReturn(new PodListBuilder().addAllToItems(pods.asJava).build())

    val top = mock(classOf[MetricAPIGroupDSL])
    val podMetricOp = mock(classOf[PodMetricOperation])
    val metricsList: PodMetricsList =
      new PodMetricsListBuilder().addAllToItems(metrics.asJava).build()
    when(client.top()).thenReturn(top)
    when(top.pods()).thenReturn(podMetricOp)
    when(podMetricOp.metrics(namespace)).thenReturn(metricsList)

    client
  }

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

  // ── namespace-wide wrappers, driven through a stubbed fabric8 client ──
  // These pin the fabric8 fluent-chain plumbing (list() / top().pods().metrics()); the value
  // transform they delegate to is already pinned by the phasesByPodName / metricsByPodName tests,
  // so they only assert that the namespace items flow through keyed by pod name.

  "getAllPodPhases" should "list the namespace pods and key them by pod name" in {
    val k8s =
      new KubernetesClient(stubbedClient(Seq(pod(1, "Running"), pod(2, "Pending")), Seq.empty))
    k8s.getAllPodPhases.keySet shouldBe
      Set(KubernetesClient.generatePodName(1), KubernetesClient.generatePodName(2))
  }

  "getAllPodMetrics" should "fetch the namespace metrics and key them by pod name" in {
    val k8s = new KubernetesClient(stubbedClient(Seq.empty, Seq(podMetrics(1, "250m", "128Mi"))))
    k8s.getAllPodMetrics.keySet shouldBe Set(KubernetesClient.generatePodName(1))
  }

  // getPodMetrics adds its own collectFirst-by-name lookup on top of the transform, so it asserts
  // both the matched pod's usage and the no-match fallback.
  "getPodMetrics" should "return the matching pod's usage and an empty map when none matches" in {
    val k8s = new KubernetesClient(stubbedClient(Seq.empty, Seq(podMetrics(1, "250m", "128Mi"))))
    k8s.getPodMetrics(1) shouldBe Map("cpu" -> "250m", "memory" -> "128Mi")
    k8s.getPodMetrics(999) shouldBe empty
  }
}
