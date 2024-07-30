package service

import config.ApplicationConf.appConfig
import io.kubernetes.client.openapi.{ApiClient, Configuration}
import io.kubernetes.client.openapi.apis.{AppsV1Api, CoreV1Api}
import io.kubernetes.client.openapi.models.{V1Container, V1ContainerPort, V1Deployment, V1DeploymentSpec, V1LabelSelector, V1ObjectMeta, V1Pod, V1PodList, V1PodSpec, V1PodTemplateSpec}
import io.kubernetes.client.util.Config
import service.KubernetesClientConfig.kubernetesConfig

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala

object KubernetesClientConfig {

  val kubernetesConfig = appConfig.kubernetes
  def createKubernetesCoreClient(): CoreV1Api = {
    val client: ApiClient = Config.fromConfig(kubernetesConfig.kubeConfigPath)
    Configuration.setDefaultApiClient(client)
    new CoreV1Api(client)
  }
  def createKubernetesAppsClient(): AppsV1Api = {
    val client: ApiClient = Config.fromConfig(kubernetesConfig.kubeConfigPath)
    Configuration.setDefaultApiClient(client)
    new AppsV1Api(client)
  }
}

class KubernetesClientService(
                               val namespace: String = kubernetesConfig.namespace,
                               val deploymentName: String = kubernetesConfig.workflowPodPoolDeploymentName) {

  // Kubernetes Api Clients are collections of different K8s objects and functions
  // which provide programmatic access to K8s resources.

  // Contains objects and resources that are core building blocks of a K8s cluster, such as pods and services.
  private val coreApi: CoreV1Api = KubernetesClientConfig.createKubernetesCoreClient()
  // Contains a set of higher level application-focused resources such as Deployments and StatefulSets.
  private val appsApi: AppsV1Api = KubernetesClientConfig.createKubernetesAppsClient()

  /**
    * Retrieves the list of pods for a given deployment in the specified namespace.
    *
    * @return A list of V1Pod objects representing the pods in the deployment.
    */
  def getPodsList(): List[V1Pod] = {
    // TODO: Get only pods from workflow-pod-pool and not all pods from defined namespace
    val request = coreApi.listNamespacedPod(namespace)
    request.execute().getItems.asScala.toList
  }

  /**
   * Retrieves the list of pods for a given label in the specified namespace.
   *
   * @param podLabel        The label of the pods to be returned.
   * @return A list of V1Pod objects representing the pods with the given label.
   */
  def getPodsList(podLabel: String): List[V1Pod] = {
    val podList = coreApi.listNamespacedPod(namespace).execute().getItems.asScala
    (
      for (
        pod: V1Pod <- podList
        if pod.getMetadata.getLabels.containsValue(podLabel)
      ) yield pod
    ).toList
  }

  /**
    * Creates a new pod under the specified deployment.
    *
    * @param podSpec        The specification of the pod to be created.
    * @return The newly created V1Pod object.
    */
  def createPod(podSpec: V1Pod): V1Pod = ???

  /**
   * Creates a new pod under the specified deployment.
   *
   * @param uid        The uid which a new pod will be created for.
   * @return The newly created V1Pod object.
   */
  def createPod(uid: Int): V1Pod = {
    val uidString: String = String.valueOf(uid)
    val deployment: V1Deployment = new V1Deployment()
      .apiVersion("apps/v1")
      .kind("Deployment")
      .metadata(
        new V1ObjectMeta()
        .name(s"user-deployment-$uid")
        .namespace(namespace)
        .labels(util.Map.of("userId", uidString))
      )
      .spec(
        new V1DeploymentSpec()
        .replicas(1)
        .selector(new V1LabelSelector().matchLabels(util.Map.of("userId", uidString)))
        .template(
          new V1PodTemplateSpec()
          .metadata(new V1ObjectMeta().labels(util.Map.of("userId", uidString))
          )
          .spec(
            new V1PodSpec().containers(util.List.of(new V1Container()
            .name("worker")
            .image("kelvinyz/python-watcher:latest")
            .ports(util.List.of(new V1ContainerPort().containerPort(8080)))))
          )
        )
      )
    appsApi.createNamespacedDeployment(namespace, deployment).execute()

    // Should be a list with a single pod
    try {
      getPodsList(uidString).last
    }
    catch {
      case e: java.util.NoSuchElementException =>
        Thread.sleep(1000)
        getPodsList(uidString).last
    }

  }

  /**
    * Deletes an existing pod with the specified pod name.
    *
    * @param podName   The name of the pod to be deleted.
    */
  def deletePod(podName: String): Unit = ???

  /**
   * Deletes an existing deployment belonging to the specific user id.
   *
   * @param uid   The deployment owner's user id.
   */
  def deleteDeployment(uid: Int): Unit = {
    appsApi.deleteNamespacedDeployment(s"user-deployment-$uid", namespace).execute()
  }

  /**
   * Find and return the latest created pod.
   *
   * @param uid        The uid which a new pod will be created for.
   * @return A newly created pod with user id attached as an annotation.
   */
  private def pollForNewPod(uid: Int): V1Pod = {
    var returnPod: V1Pod = null
    var podFound: Boolean = false

    // Loop through list of all pods in deployment to find pods without an owner (i.e. no "uid" annotation).
    // After finding owner-less pod, create "uid" annotation and set to uid parameter. Then, save the modified pod,
    // exit the loop, and return the modified pod.
    while (!podFound) {
      val podList = getPodsList(uid.toString)
      podList.foreach(pod => {
        val metadata: V1ObjectMeta = pod.getMetadata
        val annotations: java.util.Map[String, String] =
          if (metadata.getAnnotations == null) new util.HashMap[String, String]() else metadata.getAnnotations

        if (!podFound && pod.getStatus.getPhase == "Running" && !annotations.containsKey("uid")) {
          annotations.put("uid", String.valueOf(uid))
          metadata.setAnnotations(annotations)
          pod.setMetadata(metadata)

          // Possible error code 409 here.
          // Happens due to pod changing state after retrieving the initial podList,
          // leading to a mismatch between the cluster's current pod and the server's current pod object.
          // Current Solution => only consider pods which are in the "Running" state.
          // Could also add exception handling to retry in case of error.
          coreApi.replaceNamespacedPod(pod.getMetadata.getName, namespace, pod).execute()

          returnPod = pod
          podFound = true
        }
      })

      if (!podFound) {
        Thread.sleep(500)
      }
    }
    returnPod
  }
}
