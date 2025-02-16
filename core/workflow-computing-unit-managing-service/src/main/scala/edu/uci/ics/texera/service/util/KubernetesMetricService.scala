package edu.uci.ics.texera.service.util

import config.WorkflowComputingUnitManagingServiceConf
import io.fabric8.kubernetes.api.model.metrics.v1beta1.PodMetricsList
import io.fabric8.kubernetes.api.model.Quantity
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientBuilder}

import scala.jdk.CollectionConverters._

object KubernetesMetricService {

  // Initialize the Kubernetes client
  val client: KubernetesClient = new KubernetesClientBuilder().build()

  private val namespace = WorkflowComputingUnitManagingServiceConf.computeUnitPoolNamespace

  /**
   * Retrieves the pod metric for a given ID in the specified namespace.
   *
   * @param cuid  The computing unit id of the pod
   * @return The Pod metrics for a given name in a specified namespace.
   */
  def getPodMetrics(cuid: Int): Map[String, Map[String, String]] = {
    val podMetricsList: PodMetricsList = client.top().pods().metrics(namespace)
    val targetPodName = KubernetesClientService.generatePodName(cuid)

    podMetricsList.getItems.asScala.collectFirst {
      case podMetrics if podMetrics.getMetadata.getName == targetPodName =>
        podMetrics.getContainers.asScala.collectFirst {
          case container =>
            container.getUsage.asScala.collect {
              case (metric, value) =>
                println(s"Metric - $metric: ${value}")
                // CPU is in nanocores and Memory is in Kibibyte
                metric -> mapMetricWithUnit(metric, value)
            }.toMap
        }.getOrElse(Map.empty[String, Map[String, String]])
    }.getOrElse {
      println(s"No metrics found for pod: $targetPodName in namespace: $namespace")
      Map.empty[String, Map[String, String]]
    }
  }

  /**
   * Maps metric value with its associated unit, converting the unit to a more readable format.
   *
   * @param metric The name of the metric (e.g., "cpu", "memory").
   * @param quantity The value of the metric, represented as a Quantity object.
   * @return A map containing the metric value in a readable format and the corresponding unit.
   */
  private def mapMetricWithUnit(metric: String, quantity: Quantity): Map[String, String] = {
    val (value, unit) = metric match {
      case "cpu"    => (CpuUnit.convert(quantity.getAmount.toDouble, CpuUnit.Nanocores, CpuUnit.Cores), "Cores")
      case "memory" => (MemoryUnit.convert(quantity.getAmount.toDouble, MemoryUnit.Kibibyte, MemoryUnit.Mebibyte), "MiB")
      case _        => (quantity.getAmount, "unknown")
    }

    Map("value" -> value.toString, "unit" -> unit)
  }
}
