package edu.uci.ics.texera.service.util

import config.WorkflowComputingUnitManagingServiceConf
import io.fabric8.kubernetes.api.model.metrics.v1beta1.PodMetricsList
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}
import scala.jdk.CollectionConverters._

object KubernetesMetricService {

  // Initialize the Kubernetes client
  val client: KubernetesClient = new KubernetesClientBuilder().build()

  private val MEGABYTES_PER_GIGABYTE = 1024
  private val NANOCORES_PER_CPU_CORE = 1_000_000_000
  private val namespace = WorkflowComputingUnitManagingServiceConf.computeUnitPoolNamespace

  /**
   * Retrieves the pod metric for a given name in the specified namespace.
   *
   * @param cuid  The computing unit id of the pod
   * @return The Pod metrics for a given name in a specified namespace.
   */
  def getPodMetrics(cuid: Int): Map[String, Any] = {
    val podMetricsList: PodMetricsList = client.top().pods().metrics(namespace)
    val targetPodName = KubernetesClientService.generatePodName(cuid)

    podMetricsList.getItems.asScala.collectFirst {
      case podMetrics if podMetrics.getMetadata.getName == targetPodName =>
        podMetrics.getContainers.asScala.collectFirst {
          case container =>
            container.getUsage.asScala.collect {
              case ("cpu", value) =>
                val cpuInCores = convertCPUToCores(value.getAmount)
                println(s"CPU Usage: $cpuInCores cores")
                "cpu" -> cpuInCores
              case ("memory", value) =>
                val memoryInMB = convertMemoryToMB(value.getAmount)
                println(s"Memory Usage: $memoryInMB MB")
                "memory" -> memoryInMB
              case (key, value) =>
                println(s"Other Metric - $key: ${value.getAmount}")
                // Other metrics may not all be Double
                key -> value.getAmount
            }.toMap
        }.getOrElse(Map.empty[String, Double])
    }.getOrElse {
      println(s"No metrics found for pod: $targetPodName in namespace: $namespace")
      Map.empty[String, Double]
    }
  }

  private def convertMemoryToMB(memoryUsage: String): Double = {
    val memoryUsageInKi = memoryUsage.toDoubleOption.getOrElse(0.0)
    memoryUsageInKi / MEGABYTES_PER_GIGABYTE
  }

  private def convertCPUToCores(cpuUsage: String): Double = {
    val cpuUsageInNano = cpuUsage.toDoubleOption.getOrElse(0.0)
    cpuUsageInNano / NANOCORES_PER_CPU_CORE
  }
}
