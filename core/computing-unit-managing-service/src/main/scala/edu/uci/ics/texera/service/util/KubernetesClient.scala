package edu.uci.ics.texera.service.util

import edu.uci.ics.texera.service.KubernetesConfig
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.metrics.v1beta1.PodMetricsList
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}

import scala.jdk.CollectionConverters._

object KubernetesClient {

  // Initialize the Kubernetes client
  private val client: KubernetesClient = new KubernetesClientBuilder().build()
  private val namespace: String = KubernetesConfig.computeUnitPoolNamespace
  private val podNamePrefix = "computing-unit"

  def isValidQuantity(q: String): Boolean = {
    try {
      new Quantity(q)
      true
    } catch {
      case _: Exception => false
    }
  }

  def generatePodURI(cuid: Int): String = {
    s"${generatePodName(cuid)}.${KubernetesConfig.computeUnitServiceName}.$namespace.svc.cluster.local"
  }

  def generatePodName(cuid: Int): String = s"$podNamePrefix-$cuid"

  def parseCUIDFromURI(uri: String): Int = {
    val pattern = """computing-unit-(\d+).*""".r
    uri match {
      case pattern(cuid) => cuid.toInt
      case _             => throw new IllegalArgumentException(s"Invalid pod URI: $uri")
    }
  }

  def getPodsList(): List[Pod] = {
    client.pods().inNamespace(namespace).list().getItems.asScala.toList
  }

  def getPodsList(label: String): List[Pod] = {
    client.pods().inNamespace(namespace).withLabel(label).list().getItems.asScala.toList
  }

  def getPodByName(podName: String): Option[Pod] = {
    Option(client.pods().inNamespace(namespace).withName(podName).get())
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

  def createPod(
      cuid: Int,
      cpuLimit: String,
      memoryLimit: String,
      gpuLimit: String,
      envVars: Map[String, Any]
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

    // Complete the pod spec
    val pod = specBuilder
      .addNewContainer()
      .withName("computing-unit-master")
      .withImage(KubernetesConfig.computeUnitImageName)
      .addNewPort()
      .withContainerPort(KubernetesConfig.computeUnitPortNumber)
      .endPort()
      .withEnv(envList)
      .withResources(resourceBuilder.build())
      .endContainer()
      .withHostname(podName)
      .withSubdomain(KubernetesConfig.computeUnitServiceName)
      .endSpec()
      .build()

    client.pods().inNamespace(namespace).create(pod)
  }

  def deletePod(cuid: Int): Unit = {
    client.pods().inNamespace(namespace).withName(generatePodName(cuid)).delete()
  }
}
