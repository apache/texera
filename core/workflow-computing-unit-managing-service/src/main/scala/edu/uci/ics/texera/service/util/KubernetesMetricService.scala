package edu.uci.ics.texera.service.util

import config.WorkflowComputingUnitManagingServiceConf
import io.fabric8.kubernetes.api.model.metrics.v1beta1.PodMetricsList
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}
import scala.jdk.CollectionConverters._

object KubernetesMetricService {

  // Initialize the Kubernetes client
  val client: KubernetesClient = new KubernetesClientBuilder().build()

  /**
   * Retrieves the pod metric for a given name in the specified namespace.
   *
   * @param namespace The namespace of the pod to be returned
   * @param targetPodName  The name of the pod to be returned
   * @return The Pod metrics for a given name in a specified namespace.
   */
  def getPodMetrics(targetPodName: String, namespace: String): Map[String, String] = {
    val podMetricsList: PodMetricsList = client.top().pods().metrics(namespace)

    podMetricsList.getItems.asScala.collectFirst {
      case podMetrics if podMetrics.getMetadata.getName == targetPodName =>
        podMetrics.getContainers.asScala.collectFirst {
          case container =>
            val usageMap = container.getUsage.asScala.map {
              case ("cpu", value) =>
                val cpuInCores = convertCPUToCores(value.getAmount)
                println(s"CPU Usage: $cpuInCores cores")
                "cpu" -> f"$cpuInCores cores"
              case ("memory", value) =>
                val memoryInMB = convertMemoryToMB(value.getAmount)
                println(s"Memory Usage: $memoryInMB MB")
                "memory" -> f"$memoryInMB%.2f MB"
              case (key, value) =>
                println(s"Other Metric - $key: ${value.getAmount}")
                key -> value.getAmount
            }.toMap
            usageMap
        }.getOrElse(Map.empty)
    }.getOrElse {
      println(s"No metrics found for pod: $targetPodName in namespace: $namespace")
      Map.empty
    }
  }

  private def convertMemoryToMB(memoryUsage: String): Double = {
    val memoryUsageInKi = memoryUsage.toDoubleOption.getOrElse(0.0)
    memoryUsageInKi / 1024
  }

  private def convertCPUToCores(cpuUsage: String): Double = {
    val cpuUsageInNano = cpuUsage.toDoubleOption.getOrElse(0.0)
    cpuUsageInNano / 1_000_000_000
  }

//  def main(args: Array[String]): Unit = {
//    val podName: String = "computing-unit-1"
//    println(getPodMetrics(podName, WorkflowComputingUnitManagingServiceConf.computeUnitPoolNamespace).toString)
//  }
}
