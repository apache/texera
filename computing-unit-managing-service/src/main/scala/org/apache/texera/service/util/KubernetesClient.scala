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

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.metrics.v1beta1.PodMetrics
import io.fabric8.kubernetes.client.KubernetesClientBuilder
import org.apache.texera.common.config.KubernetesConfig

import scala.jdk.CollectionConverters._

object KubernetesClient {

  // Initialize the Kubernetes client
  private val client: io.fabric8.kubernetes.client.KubernetesClient =
    new KubernetesClientBuilder().build()
  private val namespace: String = KubernetesConfig.computeUnitPoolNamespace
  private val podNamePrefix = "computing-unit"

  def generatePodURI(cuid: Int): String = {
    s"${generatePodName(cuid)}.${KubernetesConfig.computeUnitServiceName}.$namespace.svc.cluster.local:${KubernetesConfig.computeUnitPortNumber}"
  }

  def generatePodName(cuid: Int): String = s"$podNamePrefix-$cuid"

  def podExists(cuid: Int): Boolean = {
    getPodByName(generatePodName(cuid)).isDefined
  }

  def getPodByName(podName: String): Option[Pod] = {
    Option(client.pods().inNamespace(namespace).withName(podName).get())
  }

  /**
    * Fetch the phase of every pod in the namespace in a single list call, keyed by pod name.
    * Intended for bulk listings (e.g. the admin view) so that N units do not trigger N separate
    * `getPodByName` round trips. Left unfiltered (rather than label-scoped) to match the
    * name-based existence semantics of `getPodByName`/`podExists`: a caller checks
    * `contains(generatePodName(cuid))` to decide whether a unit's pod still exists, and the
    * `computing-unit-<cuid>` names never collide with unrelated pods.
    *
    * A pod that exists but has no status yet maps to a `null` phase; callers can still rely
    * on `contains(podName)` to decide whether the pod exists at all.
    */
  def getAllPodPhases: Map[String, String] = {
    client
      .pods()
      .inNamespace(namespace)
      .list()
      .getItems
      .asScala
      .map(pod => pod.getMetadata.getName -> Option(pod.getStatus).map(_.getPhase).orNull)
      .toMap
  }

  // Flatten a pod's per-container resource usage into a single metric -> value map.
  private def containerUsage(podMetrics: PodMetrics): Map[String, String] =
    podMetrics.getContainers.asScala.flatMap { container =>
      container.getUsage.asScala.map {
        case (metric, value) => metric -> value.toString
      }
    }.toMap

  // One namespace-wide `top` call, returning the raw per-pod metrics items.
  private def fetchPodMetricsItems(): Iterable[PodMetrics] =
    client.top().pods().metrics(namespace).getItems.asScala

  /**
    * Fetch CPU/memory usage for every pod in the namespace in a single `top` call, keyed by
    * pod name. Intended for bulk listings so that N units do not each re-fetch the whole
    * namespace metrics list (which `getPodMetrics` does per call).
    */
  def getAllPodMetrics: Map[String, Map[String, String]] =
    fetchPodMetricsItems()
      .map(podMetrics => podMetrics.getMetadata.getName -> containerUsage(podMetrics))
      .toMap

  def getPodMetrics(cuid: Int): Map[String, String] = {
    val targetPodName = generatePodName(cuid)
    fetchPodMetricsItems()
      .collectFirst {
        case podMetrics if podMetrics.getMetadata.getName == targetPodName =>
          containerUsage(podMetrics)
      }
      .getOrElse(Map.empty[String, String])
  }

  def getPodLimits(cuid: Int): Map[String, String] = {
    getPodByName(generatePodName(cuid))
      .flatMap { pod =>
        pod.getSpec.getContainers.asScala.headOption.map { container =>
          val limitsMap = container.getResources.getLimits.asScala.map {
            case (key, value) => key -> value.toString
          }.toMap

          limitsMap
        }
      }
      .getOrElse(Map.empty[String, String])
  }

  def createPod(
      cuid: Int,
      cpuLimit: String,
      memoryLimit: String,
      gpuLimit: String,
      envVars: Map[String, Any],
      shmSize: Option[String] = None
  ): Pod = {
    val podName = generatePodName(cuid)
    if (getPodByName(podName).isDefined) {
      throw new Exception(s"Pod with cuid $cuid already exists")
    }

    val envList = envVars
      .map {
        case (key, value) =>
          new EnvVarBuilder()
            .withName(key)
            .withValue(value.toString)
            .build()
      }
      .toList
      .asJava

    // Setup the resource requirements
    val resourceBuilder = new ResourceRequirementsBuilder()
      .addToLimits("cpu", new Quantity(cpuLimit))
      .addToLimits("memory", new Quantity(memoryLimit))

    // Only add GPU resources if the requested amount is greater than 0
    if (gpuLimit != "0") {
      // Use the configured GPU resource key directly
      resourceBuilder.addToLimits(KubernetesConfig.gpuResourceKey, new Quantity(gpuLimit))
    }

    // Build the pod with metadata
    val podBuilder = new PodBuilder()
      .withNewMetadata()
      .withName(podName)
      .withNamespace(namespace)
      .addToLabels("type", "computing-unit")
      .addToLabels("cuid", cuid.toString)
      .addToLabels("name", podName)

    // Start building the pod spec
    val specBuilder = podBuilder
      .endMetadata()
      .withNewSpec()

    // Only add runtimeClassName when using NVIDIA GPU
    if (gpuLimit != "0" && KubernetesConfig.gpuResourceKey.contains("nvidia")) {
      specBuilder.withRuntimeClassName("nvidia")
    }

    val containerBuilder = specBuilder
      .addNewContainer()
      .withName("computing-unit-master")
      .withImage(KubernetesConfig.computeUnitImageName)
      .withImagePullPolicy(KubernetesConfig.computingUnitImagePullPolicy)
      .addNewPort()
      .withContainerPort(KubernetesConfig.computeUnitPortNumber)
      .endPort()
      .withEnv(envList)
      .withResources(resourceBuilder.build())

    // If shmSize requested, mount /dev/shm
    shmSize.foreach { _ =>
      containerBuilder
        .addNewVolumeMount()
        .withName("dshm")
        .withMountPath("/dev/shm")
        .endVolumeMount()
    }

    containerBuilder.endContainer()

    // Add tmpfs volume if needed
    shmSize.foreach { size =>
      specBuilder
        .addNewVolume()
        .withName("dshm")
        .withEmptyDir(
          new EmptyDirVolumeSourceBuilder()
            .withMedium("Memory")
            .withSizeLimit(new Quantity(size))
            .build()
        )
        .endVolume()
    }

    val pod = specBuilder
      .withHostname(podName)
      .withSubdomain(KubernetesConfig.computeUnitServiceName)
      .endSpec()
      .build()

    client.resource(pod).inNamespace(namespace).create()
  }

  def deletePod(cuid: Int): Unit = {
    client.pods().inNamespace(namespace).withName(generatePodName(cuid)).delete()
  }
}
