package edu.uci.ics.texera.service.util

import config.WorkflowComputingUnitManagingServiceConf
import io.fabric8.kubernetes.api.model.metrics.v1beta1.PodMetricsList
import io.fabric8.kubernetes.api.model.PodList
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
  def getPodMetrics(cuid: Int): Map[String, String] = {
    val podMetricsList: PodMetricsList = client.top().pods().metrics(namespace)
    val targetPodName = KubernetesClientService.generatePodName(cuid)

    podMetricsList.getItems.asScala
      .collectFirst {
        case podMetrics if podMetrics.getMetadata.getName == targetPodName =>
          podMetrics.getContainers.asScala
            .collectFirst {
              case container =>
                container.getUsage.asScala.collect {
                  case (metric, value) =>
                    println(s"Metric - $metric: ${value}")
                    // CPU is in nanocores and Memory is in Kibibyte
                    metric -> value.toString
                }.toMap
            }
            .getOrElse(Map.empty[String, String])
      }
      .getOrElse {
        println(s"No metrics found for pod: $targetPodName in namespace: $namespace")
        Map.empty[String, String]
      }
  }

  /**
    * Retrieves the pod limits for a given ID in the specified namespace.
    *
    * @param cuid  The computing unit id of the pod
    * @return The Pod limits for a given name in a specified namespace.
    */
  def getPodLimits(cuid: Int): Map[String, String] = {
    val podList: PodList = client.pods().inNamespace(namespace).list()
    val targetPodName = KubernetesClientService.generatePodName(cuid)

    val pod = podList.getItems.asScala.find(pod => {
      pod.getMetadata.getName.equals(targetPodName)
    })

    val limits: Map[String, String] = pod
      .flatMap { pod =>
        pod.getSpec.getContainers.asScala.headOption.map { container =>
          container.getResources.getLimits.asScala.map {
            case (key, value) =>
              key -> value.toString
          }.toMap
        }
      }
      .getOrElse(Map.empty[String, String])
    println(limits.toString())
    limits
  }
}
