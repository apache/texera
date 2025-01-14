package edu.uci.ics.texera.service.util

import config.WorkflowComputingUnitManagingServiceConf
import config.WorkflowComputingUnitManagingServiceConf.{
  computeUnitImageName,
  computeUnitPortNumber,
  computeUnitServiceName
}
import edu.uci.ics.amber.core.storage.StorageConfig
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models._
import io.kubernetes.client.openapi.{ApiClient, Configuration}
import io.kubernetes.client.util.Config

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala

object KubernetesClientService {

  private val podNamePrefix = "computing-unit"
  // Create Kubernetes Core and Apps clients
  private val coreApi: CoreV1Api = {
    val client: ApiClient = Config.defaultClient()
    Configuration.setDefaultApiClient(client)
    new CoreV1Api(client)
  }

  private val poolNamespace: String =
    WorkflowComputingUnitManagingServiceConf.computeUnitPoolNamespace

  /**
    * Generates a URI for the pod based on the computing unit ID (cuid).
    *
    * @param cuid The computing unit ID.
    * @return A URI representing the pod location.
    */
  def generatePodURI(cuid: Int): String = {
    s"${generatePodName(cuid)}.$computeUnitServiceName.$poolNamespace.svc.cluster.local"
  }

  /**
    * Generate pod name using the cuid
    *
    * @param cuid The computing unit ID
    * @return The pod name
    */
  def generatePodName(cuid: Int): String = s"$podNamePrefix-$cuid"

  /**
    * Parses the computing unit ID (cuid) from a given pod URI.
    *
    * @param uri The pod URI.
    * @return The extracted computing unit ID as an integer.
    */
  def parseCUIDFromURI(uri: String): Int = {
    val pattern = """computing-unit-(\d+).*""".r
    uri match {
      case pattern(cuid) => cuid.toInt
      case _             => throw new IllegalArgumentException(s"Invalid pod URI: $uri")
    }
  }

  /**
    * Retrieves the list of all pods in the specified namespace.
    *
    * @param namespace The namespace of the pods to be returned.
    * @return A list of V1Pod objects.
    */
  def getPodsList(namespace: String): List[V1Pod] = {
    coreApi.listNamespacedPod(namespace).execute().getItems.asScala.toList
  }

  /**
    * Retrieves the list of pods for a given label in the specified namespace.
    *
    * @param namespace The namespace of the pods to be returned.
    * @param podLabel  The label of the pods to be returned.
    * @return A list of V1Pod objects representing the pods with the given label.
    */
  def getPodsList(namespace: String, podLabel: String): List[V1Pod] = {
    coreApi.listNamespacedPod(namespace).labelSelector(podLabel).execute().getItems.asScala.toList
  }

  /**
    * Retrieves a single pod with the given label in the specified namespace.
    *
    * @param namespace The namespace of the pod to be returned.
    * @param podLabel  The label of the pod to be returned.
    * @return A V1Pod object representing the pod with the given label.
    */
  def getPodFromLabel(namespace: String, podLabel: String): V1Pod = {
    val podsList = getPodsList(namespace, podLabel)
    if (podsList.isEmpty) {
      null
    } else {
      podsList.last
    }
  }

  /**
    * Checks if the pod is in the desired status.
    *
    * @param podName      The name of the pod.
    * @param desiredState The desired state.
    * @return Boolean indicating if the pod is in the desired state.
    */
  private def isPodInDesiredState(podName: String, desiredState: String): Boolean = {
    val pod = coreApi.readNamespacedPod(podName, poolNamespace).execute()
    println(pod.getStatus.getPhase)
    pod.getStatus.getPhase == desiredState
  }

  def getPodByName(podName: String): V1Pod = {
    coreApi.readNamespacedPod(podName, poolNamespace).execute()
  }

  /**
    * Creates a new pod under the specified namespace for the given computing unit ID.
    *
    * @param cuid The computing unit ID.
    * @return The newly created V1Pod object.
    */
  def createPod(cuid: Int): V1Pod = {
    val podName = generatePodName(cuid)
    if (getPodFromLabel(poolNamespace, s"name=$podName") != null) {
      throw new Exception(s"Pod with cuid $cuid already exists")
    }

    val pod: V1Pod = new V1Pod()
      .apiVersion("v1")
      .kind("Pod")
      .metadata(
        new V1ObjectMeta()
          .name(podName)
          .namespace(poolNamespace)
          .labels(
            util.Map.of(
              "type",
              "computing-unit",
              "cuid",
              String.valueOf(cuid),
              "name",
              podName
            )
          )
      )
      .spec(
        new V1PodSpec()
          .overhead(null) // https://github.com/kubernetes-client/java/issues/3076
          .containers(
            util.List.of(
              new V1Container()
                .name("computing-unit-master")
                .image(computeUnitImageName)
                .ports(util.List.of(new V1ContainerPort().containerPort(computeUnitPortNumber)))
                .env(
                  util.List.of(
                    new V1EnvVar().name("JDBC_URL").value(StorageConfig.jdbcUrl),
                    new V1EnvVar().name("JDBC_USERNAME").value(StorageConfig.jdbcUsername),
                    new V1EnvVar().name("JDBC_PASSWORD").value(StorageConfig.jdbcPassword)
                  )
                )
            )
          )
          .hostname(podName)
          .subdomain(computeUnitServiceName)
      )

    coreApi.createNamespacedPod(poolNamespace, pod).execute()
  }

  /**
    * Deletes an existing pod using the pod URI.
    *
    * @param podURI The URI of the pod to delete.
    */
  def deletePod(podURI: String): Unit = {
    val cuid = parseCUIDFromURI(podURI)
    coreApi.deleteNamespacedPod(generatePodName(cuid), poolNamespace).execute()
  }

  /**
    * Waits for the pod to reach the desired status.
    *
    * @param cuid          The computing unit ID.
    * @param desiredStatus The desired pod status.
    */
  private def waitForPodStatus(cuid: Int, desiredStatus: String): Unit = {
    var attempts = 0
    val maxAttempts = 60
    val podName = generatePodName(cuid)
    while (attempts < maxAttempts && !isPodInDesiredState(podName, desiredStatus)) {
      attempts += 1
      Thread.sleep(1000)
      println(s"Waiting for pod $podName to reach $desiredStatus (attempt $attempts)")
    }

    if (!isPodInDesiredState(podName, desiredStatus)) {
      throw new RuntimeException(
        s"Pod $podName failed to reach $desiredStatus after $maxAttempts attempts"
      )
    }
  }
}
