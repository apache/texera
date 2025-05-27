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

package edu.uci.ics.texera.service.util

import edu.uci.ics.texera.service.KubernetesConfig
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.apps.{StatefulSet, StatefulSetBuilder, StatefulSetSpecBuilder}
import io.fabric8.kubernetes.api.model.metrics.v1beta1.PodMetricsList
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}

import scala.jdk.CollectionConverters._

object KubernetesClient {

  // Initialize the Kubernetes client
  private val client: KubernetesClient = new KubernetesClientBuilder().build()
  private val namespace: String = KubernetesConfig.computeUnitPoolNamespace
  private val podNamePrefix = "computing-unit"

  def generatePodURI(cuid: Int): String = {
    s"${generatePodName(cuid)}.${KubernetesConfig.computeUnitServiceName}.$namespace.svc.cluster.local"
  }

  def generatePodName(cuid: Int): String = s"$podNamePrefix-$cuid"

  def generateVolumeName(cuid:Int) = s"${generatePodName(cuid)}-pvc"

  def generateClusterMasterServiceName(cuid:Int) = s"${generatePodName(cuid)}-master"

  def generateStatefulSetName(cuid: Int): String = s"${generatePodName(cuid)}-workers"

  def getPodByName(podName: String): Option[Pod] = {
    Option(client.pods().inNamespace(namespace).withName(podName).get())
  }

  def getClusterPodsById(cuid: Int): Array[Pod] = {
    client
      .pods()
      .inNamespace(namespace)
      .withLabel("type", "computing-unit")
      .withLabel("cuid", cuid.toString)
      .list()
      .getItems
      .asScala
      .toArray
  }

  def getPodMetrics(cuid: Int): Map[String, String] = {
    val podMetricsList: PodMetricsList = client.top().pods().metrics(namespace)
    val targetPodName = generatePodName(cuid)

    podMetricsList.getItems.asScala
      .collectFirst {
        case podMetrics if podMetrics.getMetadata.getName == targetPodName =>
          podMetrics.getContainers.asScala.flatMap { container =>
            container.getUsage.asScala.map {
              case (metric, value) =>
                metric -> value.toString
            }
          }.toMap
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


  // ---------------------------------------------------------------------------
  // Cluster lifecycle helpers
  // ---------------------------------------------------------------------------

  def createVolume(cuid: Int, diskLimit: String): Volume = {
    val pvcName = generateVolumeName(cuid)

    // Build / create PVC if it doesn't exist yet
    val pvc = new PersistentVolumeClaimBuilder()
      .withNewMetadata()
      .withName(pvcName)
      .withNamespace(namespace)
      .addToLabels("type", "computing-unit")
      .addToLabels("cuid", cuid.toString)
      .endMetadata()
      .withNewSpec()
      .withAccessModes("ReadWriteOnce")
      .withNewResources()
      .addToRequests("storage", new Quantity(diskLimit))
      .endResources()
      .withStorageClassName(KubernetesConfig.computingUnitStorageClassName)
      .endSpec()
      .build()

    // idempotent create / update
    client.persistentVolumeClaims().inNamespace(namespace).create(pvc)

    // Return a Volume that points to the PVC so callers can mount it
    new VolumeBuilder()
      .withName(pvcName)
      .withNewPersistentVolumeClaim()
      .withClaimName(pvcName)
      .endPersistentVolumeClaim()
      .build()
  }


  def createCluster(
                     cuid: Int,
                     cpuLimit: String,
                     memoryLimit: String,
                     diskLimit: String,
                     numNodes: Int,
                     envVars: Map[String, Any]
                   ): Pod = {
    val masterIp    = generatePodURI(cuid)
    val enrichedEnv = envVars ++ Map(
      "CLUSTERING_ENABLED"           -> "true",
      "CLUSTERING_MASTER_IP_ADDRESS" -> masterIp
    )
    val volume = createVolume(cuid, diskLimit)
    val master = createPod(cuid, cpuLimit, memoryLimit, gpuLimit = "0", enrichedEnv, Some(volume))
    createClusterMasterService(cuid)
    createStatefulSet(cuid, cpuLimit, memoryLimit, numNodes - 1, enrichedEnv, volume)
    master // return master pod
  }

  def deleteCluster(cuid: Int): Unit = {
    deletePod(cuid)
    deleteClusterMasterService(cuid)
    deleteStatefulSet(cuid)
    deleteVolume(cuid)
  }

  // ---------------------------------------------------------------------------
  // Kubernetes resource creators
  // ---------------------------------------------------------------------------

  private def createClusterMasterService(cuid: Int): Service = {
    val serviceName = generateClusterMasterServiceName(cuid)
    val service = new ServiceBuilder()
      .withNewMetadata()
      .withName(serviceName)
      .withNamespace(namespace)
      .endMetadata()
      .withNewSpec()
      .withClusterIP("None")                     // headless for DNS discovery
      .addNewPort()
      .withPort(2552)
      .endPort()
      .addToSelector("type", "computing-unit")
      .addToSelector("cuid", cuid.toString)
      .addToSelector("role", "master")
      .endSpec()
      .build()

    client.services().inNamespace(namespace).create(service)
  }

  private def createStatefulSet(
                                 cuid: Int,
                                 cpuLimit: String,
                                 memoryLimit: String,
                                 numNodes: Int,
                                 envVars: Map[String, Any],
                                 volume: Volume
                               ): StatefulSet = {
    val envList = envVars.map { case (k, v) =>
      new EnvVarBuilder().withName(k).withValue(v.toString).build()
    }.toList.asJava

    val resources = new ResourceRequirementsBuilder()
      .addToLimits("cpu", new Quantity(cpuLimit))
      .addToLimits("memory", new Quantity(memoryLimit))
      .build()

    val container = new ContainerBuilder()
      .withName("computing-unit-worker")
      .withImage(KubernetesConfig.computeUnitWorkerImageName)
      .withImagePullPolicy(KubernetesConfig.computingUnitImagePullPolicy)
      .addNewVolumeMount().withName(volume.getName).withMountPath("/core/amber/user-resources").endVolumeMount()
      .addNewPort().withContainerPort(KubernetesConfig.computeUnitPortNumber).endPort()
      .withEnv(envList)
      .withResources(resources)
      .build()

    val sts = new StatefulSetBuilder()
      .withNewMetadata()
      .withName(generateStatefulSetName(cuid))
      .withNamespace(namespace)
      .endMetadata()
      .withSpec(
        new StatefulSetSpecBuilder()
          .withServiceName(generatePodName(cuid))
          .withReplicas(numNodes)
          .withSelector(
            new LabelSelectorBuilder()
              .addToMatchLabels("type", "computing-unit")
              .addToMatchLabels("cuid", cuid.toString)
              .addToMatchLabels("role", "worker")
              .build()
          )
          .withNewTemplate()
          .withNewMetadata()
          .addToLabels("type", "computing-unit")
          .addToLabels("cuid", cuid.toString)
          .addToLabels("role", "worker")
          .endMetadata()
          .withNewSpec()
          .withContainers(container)
          .endSpec()
          .endTemplate()
          .build()
      )
      .build()

    client.apps().statefulSets().inNamespace(namespace).create(sts)
  }

  def createPod(
      cuid: Int,
      cpuLimit: String,
      memoryLimit: String,
      gpuLimit: String,
      envVars: Map[String, Any],
      attachVolume: Option[Volume] = None
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
      .addToLabels("role", "master")

    // -------------- CONTAINER -------------
    val containerB = new ContainerBuilder()
      .withName("computing-unit-master")
      .withImage(KubernetesConfig.computeUnitMasterImageName)
      .withImagePullPolicy(KubernetesConfig.computingUnitImagePullPolicy)
      .addNewPort().withContainerPort(KubernetesConfig.computeUnitPortNumber).endPort()
      .withEnv(envList)
      .withResources(resourceBuilder.build())

    // Start building the pod spec
    val specBuilder = podBuilder
      .endMetadata()
      .withNewSpec()

    // mount PVC at /data if provided
    attachVolume.foreach { v =>
      containerB.addNewVolumeMount().withName(v.getName).withMountPath("/core/amber/user-resources").endVolumeMount()
      specBuilder.addToVolumes(v)
    }

    val container = containerB.build()

    // Only add runtimeClassName when using NVIDIA GPU
    if (gpuLimit != "0" && KubernetesConfig.gpuResourceKey.contains("nvidia")) {
      specBuilder.withRuntimeClassName("nvidia")
    }

    // Complete the pod spec
    val pod = specBuilder
      .withContainers(container)
      .withHostname(podName)
      .withSubdomain(KubernetesConfig.computeUnitServiceName)
      .endSpec()
      .build()

    client.pods().inNamespace(namespace).create(pod)
  }

  def deletePod(cuid: Int): Unit = {
    client.pods().inNamespace(namespace).withName(generatePodName(cuid)).delete()
  }


  private def deleteVolume(cuid: Int): Unit = {
    client.persistentVolumeClaims().inNamespace(namespace).withName(generateVolumeName(cuid)).delete()
  }

  private def deleteClusterMasterService(cuid: Int): Unit =
    client.services().inNamespace(namespace).withName(generateClusterMasterServiceName(cuid)).delete()

  private def deleteStatefulSet(cuid: Int): Unit =
    client.apps().statefulSets().inNamespace(namespace).withName(generateStatefulSetName(cuid)).delete()
}
