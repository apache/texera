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
import io.fabric8.kubernetes.api.model.{
  ContainerBuilder,
  Pod,
  PodBuilder,
  PodList,
  PodListBuilder,
  Quantity,
  ResourceRequirementsBuilder
}
import io.fabric8.kubernetes.client.dsl.{
  MetricAPIGroupDSL,
  MixedOperation,
  NamespaceableResource,
  NonNamespaceOperation,
  PodMetricOperation,
  PodResource,
  Resource
}
import io.fabric8.kubernetes.client.{KubernetesClient => Fabric8Client}
import org.apache.texera.common.config.KubernetesConfig
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, times, verify, when}
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
  // ── single-pod lookups, creation and deletion ──
  // These reach the rest of the fluent chain: withName(...).get() for the lookups,
  // resource(pod).inNamespace(...).create() for creation, and .delete() for removal. Everything
  // is driven through the constructor seam, so no cluster is involved.

  /** Extends the namespace stub with the by-name pod operations `withName(...)` returns. */
  private def clientWithNamedPod(podName: String, found: Pod): (Fabric8Client, PodResource) = {
    val client = stubbedClient(Seq.empty, Seq.empty)
    val podsInNamespace = client.pods().inNamespace(namespace)
    val podResource = mock(classOf[PodResource])
    when(podsInNamespace.withName(podName)).thenReturn(podResource)
    when(podResource.get()).thenReturn(found)
    (client, podResource)
  }

  /** A pod carrying one container whose resource limits are set. */
  private def podWithLimits(cuid: Int, limits: Map[String, String]): Pod =
    new PodBuilder()
      .withNewMetadata()
      .withName(KubernetesClient.generatePodName(cuid))
      .endMetadata()
      .withNewSpec()
      .addToContainers(
        new ContainerBuilder()
          .withName("main")
          .withResources(
            new ResourceRequirementsBuilder()
              .withLimits(limits.map { case (k, v) => k -> new Quantity(v) }.asJava)
              .build()
          )
          .build()
      )
      .endSpec()
      .build()

  "generatePodURI" should "address the pod through its headless service inside the namespace" in {
    // The URI is how a computing unit is reached once it is up, so every segment matters: a pod
    // name alone, or the wrong namespace, resolves to nothing.
    val uri = KubernetesClient.generatePodURI(7)
    uri should startWith(KubernetesClient.generatePodName(7) + ".")
    uri should include(s".${KubernetesConfig.computeUnitServiceName}.$namespace.svc.cluster.local:")
    uri should endWith(s":${KubernetesConfig.computeUnitPortNumber}")
  }

  "getPodByName" should "wrap a found pod and report a missing one as None" in {
    // fabric8 returns null rather than throwing for an absent pod, so the Option() wrapper is the
    // only thing standing between a caller and an NPE.
    val name = KubernetesClient.generatePodName(1)
    val (found, _) = clientWithNamedPod(name, pod(1, "Running"))
    val (absent, _) = clientWithNamedPod(name, null)

    new KubernetesClient(found).getPodByName(name).map(_.getMetadata.getName) shouldBe Some(name)
    new KubernetesClient(absent).getPodByName(name) shouldBe None
  }

  "podExists" should "follow the by-name lookup in both directions" in {
    val name = KubernetesClient.generatePodName(2)
    new KubernetesClient(clientWithNamedPod(name, pod(2, "Running"))._1).podExists(2) shouldBe true
    new KubernetesClient(clientWithNamedPod(name, null)._1).podExists(2) shouldBe false
  }

  "getPodLimits" should "read the first container's limits and fall back to an empty map" in {
    val name = KubernetesClient.generatePodName(3)
    val withLimits =
      clientWithNamedPod(name, podWithLimits(3, Map("cpu" -> "2", "memory" -> "4Gi")))._1
    val missing = clientWithNamedPod(name, null)._1

    new KubernetesClient(withLimits).getPodLimits(3) shouldBe Map("cpu" -> "2", "memory" -> "4Gi")
    new KubernetesClient(missing).getPodLimits(3) shouldBe empty
  }

  "createPod" should "refuse to overwrite a pod that already exists" in {
    // Creating over a live unit would silently detach the running one from its owner.
    val name = KubernetesClient.generatePodName(4)
    val k8s = new KubernetesClient(clientWithNamedPod(name, pod(4, "Running"))._1)

    val thrown = intercept[Exception] {
      k8s.createPod(4, "1", "2Gi", "0", Map.empty)
    }
    thrown.getMessage should include("already exists")
  }

  it should "build the pod from the requested limits and env, and create it in the namespace" in {
    val name = KubernetesClient.generatePodName(5)
    val (client, _) = clientWithNamedPod(name, null)
    val namespaceable = mock(classOf[NamespaceableResource[Pod]])
    val resource = mock(classOf[Resource[Pod]])
    val captor = ArgumentCaptor.forClass(classOf[Pod])
    when(client.resource(any(classOf[Pod]))).thenReturn(namespaceable)
    when(namespaceable.inNamespace(namespace)).thenReturn(resource)
    // create()'s return value is not asserted; the pod is inspected through the captor below.
    when(resource.create()).thenReturn(null)

    new KubernetesClient(client).createPod(5, "2", "4Gi", "1", Map("UID" -> 9, "MODE" -> "batch"))

    verify(client).resource(captor.capture())
    val built = captor.getValue
    built.getSpec.getHostname shouldBe name
    built.getSpec.getSubdomain shouldBe KubernetesConfig.computeUnitServiceName
    val container = built.getSpec.getContainers.asScala.head
    val limits = container.getResources.getLimits.asScala.map { case (k, v) => k -> v.toString }
    limits("cpu") shouldBe "2"
    limits("memory") shouldBe "4Gi"
    // Env values arrive as Any and reach the container as strings.
    container.getEnv.asScala.map(e => e.getName -> e.getValue).toMap shouldBe
      Map("UID" -> "9", "MODE" -> "batch")
  }

  it should "mount a shared-memory volume only when a size is asked for" in {
    // /dev/shm defaults to 64Mi in Kubernetes, which is too small for the Python workers, so the
    // volume is the fix — but it must not appear when no size was requested.
    def build(shm: Option[String]): Pod = {
      val name = KubernetesClient.generatePodName(6)
      val (client, _) = clientWithNamedPod(name, null)
      val namespaceable = mock(classOf[NamespaceableResource[Pod]])
      val resource = mock(classOf[Resource[Pod]])
      val captor = ArgumentCaptor.forClass(classOf[Pod])
      when(client.resource(any(classOf[Pod]))).thenReturn(namespaceable)
      when(namespaceable.inNamespace(namespace)).thenReturn(resource)
      // create()'s return value is not asserted; the pod is inspected through the captor below.
      when(resource.create()).thenReturn(null)
      new KubernetesClient(client).createPod(6, "1", "2Gi", "0", Map.empty, shm)
      verify(client).resource(captor.capture())
      captor.getValue
    }

    val withShm = build(Some("1Gi"))
    withShm.getSpec.getVolumes.asScala.map(_.getName) should contain("dshm")
    withShm.getSpec.getVolumes.asScala
      .find(_.getName == "dshm")
      .flatMap(v => Option(v.getEmptyDir))
      .map(_.getSizeLimit.toString) shouldBe Some("1Gi")

    Option(build(None).getSpec.getVolumes).map(_.asScala.map(_.getName)).getOrElse(Nil) should
      not contain "dshm"
  }

  "deletePod" should "delete the pod for the cuid inside the namespace" in {
    val name = KubernetesClient.generatePodName(8)
    val (client, podResource) = clientWithNamedPod(name, pod(8, "Running"))

    new KubernetesClient(client).deletePod(8)

    verify(podResource, times(1)).delete()
  }
}
