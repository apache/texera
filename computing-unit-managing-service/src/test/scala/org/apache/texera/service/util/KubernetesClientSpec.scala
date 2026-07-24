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
import io.fabric8.kubernetes.client.{KubernetesClientBuilder, KubernetesClient => Fabric8Client}
import org.apache.texera.common.config.KubernetesConfig
import org.apache.texera.dao.jooq.generated.enums.WorkflowComputingUnitTypeEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import org.mockito.Mockito.{mock, when}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

// Exercises the fabric8 wrappers without a cluster by stubbing the client with Mockito.
class KubernetesClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val namespace: String = KubernetesConfig.computeUnitPoolNamespace

  private def unitOfType(cuid: Int, tpe: WorkflowComputingUnitTypeEnum): WorkflowComputingUnit = {
    val u = new WorkflowComputingUnit()
    u.setCuid(cuid)
    u.setType(tpe)
    u
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

  // cuid 1 -> Running, cuid 2 -> Pending; both returned by the namespace-wide listing.
  private val podList: PodList =
    new PodListBuilder().addToItems(pod(1, "Running"), pod(2, "Pending")).build()

  // Only cuid 1 reports metrics.
  private val metricsList: PodMetricsList =
    new PodMetricsListBuilder()
      .addToItems(
        new PodMetricsBuilder()
          .withNewMetadata()
          .withName(KubernetesClient.generatePodName(1))
          .endMetadata()
          .addToContainers(
            new ContainerMetricsBuilder()
              .withName("main")
              .withUsage(
                Map("cpu" -> new Quantity("250m"), "memory" -> new Quantity("128Mi")).asJava
              )
              .build()
          )
          .build()
      )
      .build()

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val client = mock(classOf[Fabric8Client])

    val podsMixed = mock(classOf[MixedOperation[_, _, _]])
      .asInstanceOf[MixedOperation[Pod, PodList, PodResource]]
    val podsInNamespace = mock(classOf[NonNamespaceOperation[_, _, _]])
      .asInstanceOf[NonNamespaceOperation[Pod, PodList, PodResource]]
    when(client.pods()).thenReturn(podsMixed)
    when(podsMixed.inNamespace(namespace)).thenReturn(podsInNamespace)
    when(podsInNamespace.list()).thenReturn(podList)

    // cuid 1 -> a present pod; cuid 2 -> no pod.
    val presentResource = mock(classOf[PodResource])
    when(presentResource.get()).thenReturn(pod(1, "Running"))
    val absentResource = mock(classOf[PodResource])
    when(absentResource.get()).thenReturn(null)
    when(podsInNamespace.withName(KubernetesClient.generatePodName(1))).thenReturn(presentResource)
    when(podsInNamespace.withName(KubernetesClient.generatePodName(2))).thenReturn(absentResource)

    val top = mock(classOf[MetricAPIGroupDSL])
    val podMetricOp = mock(classOf[PodMetricOperation])
    when(client.top()).thenReturn(top)
    when(top.pods()).thenReturn(podMetricOp)
    when(podMetricOp.metrics(namespace)).thenReturn(metricsList)

    KubernetesClient.setClientForTesting(client)
  }

  override protected def afterAll(): Unit = {
    try {
      // Restore a real client so subsequent suites don't reuse the mock.
      KubernetesClient.setClientForTesting(new KubernetesClientBuilder().build())
    } finally super.afterAll()
  }

  "generatePodName" should "prefix the cuid with computing-unit" in {
    KubernetesClient.generatePodName(42) shouldBe "computing-unit-42"
  }

  it should "handle a cuid of 0" in {
    KubernetesClient.generatePodName(0) shouldBe "computing-unit-0"
  }

  "getPodByName" should "return the pod when present and None when absent" in {
    KubernetesClient.getPodByName(KubernetesClient.generatePodName(1)) shouldBe defined
    KubernetesClient.getPodByName(KubernetesClient.generatePodName(2)) shouldBe None
  }

  "podExists" should "reflect whether the pod is present" in {
    KubernetesClient.podExists(1) shouldBe true
    KubernetesClient.podExists(2) shouldBe false
  }

  "getAllPodPhases" should "map every pod name in the namespace to its phase" in {
    val phases = KubernetesClient.getAllPodPhases
    phases(KubernetesClient.generatePodName(1)) shouldBe "Running"
    phases(KubernetesClient.generatePodName(2)) shouldBe "Pending"
  }

  "getAllPodMetrics" should "map pod names to their cpu/memory usage" in {
    val metrics = KubernetesClient.getAllPodMetrics
    metrics(KubernetesClient.generatePodName(1)) shouldBe Map("cpu" -> "250m", "memory" -> "128Mi")
  }

  "getPodMetrics" should "return the usage for the pod matching the cuid" in {
    KubernetesClient.getPodMetrics(1) shouldBe Map("cpu" -> "250m", "memory" -> "128Mi")
  }

  it should "return an empty map when no pod matches the cuid" in {
    KubernetesClient.getPodMetrics(999) shouldBe empty
  }

  "ComputingUnitHelpers.podPhasesFor" should "query the cluster iff a kubernetes unit is present" in {
    ComputingUnitHelpers.podPhasesFor(
      List(unitOfType(5, WorkflowComputingUnitTypeEnum.local))
    ) shouldBe empty

    ComputingUnitHelpers.podPhasesFor(
      List(unitOfType(1, WorkflowComputingUnitTypeEnum.kubernetes))
    )(KubernetesClient.generatePodName(1)) shouldBe "Running"
  }

  "ComputingUnitHelpers.podMetricsFor" should "query the cluster iff a kubernetes unit is present" in {
    ComputingUnitHelpers.podMetricsFor(
      List(unitOfType(5, WorkflowComputingUnitTypeEnum.local))
    ) shouldBe empty

    ComputingUnitHelpers.podMetricsFor(
      List(unitOfType(1, WorkflowComputingUnitTypeEnum.kubernetes))
    )(KubernetesClient.generatePodName(1)) shouldBe Map("cpu" -> "250m", "memory" -> "128Mi")
  }
}
