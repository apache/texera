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
    * Retrieves the list of all pods in the specified namespace.
    *
    * @return A list of V1Pod objects.
    */
  def getPodsList(): List[V1Pod] = {
    coreApi.listNamespacedPod(namespace).execute().getItems.asScala.toList
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
   * Creates a new pod under the specified namespace.
   *
   * @param uid        The uid which a new pod will be created for.
   * @return The newly created V1Pod object.
   */
  def createPod(uid: Int): V1Pod = {
    val uidString: String = String.valueOf(uid)
    val pod: V1Pod = createUserPod(uid)

    // Should be a list with a single pod
    try {
      getPodsList(uidString).last
    }
    catch {
      // In case program moves too fast and newly created pod is not detectable yet
      case e: java.util.NoSuchElementException =>
        println(e.getMessage)
        Thread.sleep(1000)
        println("Attempting to retrieve pod again")
        getPodsList(uidString).last
    }
  }

  /**
   * Creates a pod belonging to the specified user id.
   *
   * @param uid        The uid which a pod pod will be created for.
   * @return The newly created V1Pod object.
   */
  def createUserPod(uid: Int): V1Pod = {
    val uidString: String = String.valueOf(uid)
    val pod: V1Pod = new V1Pod()
      .apiVersion("v1")
      .kind("Pod")
      .metadata(
        new V1ObjectMeta()
          .name(s"user-pod-$uid")
          .namespace(namespace)
          .labels(util.Map.of("userId", uidString, "workflow", "worker"))
      )
      .spec(
        new V1PodSpec()
          .containers(
            util.List.of(new V1Container()
              .name("worker")
              .image("pureblank/dropwizard-example:latest")
              .ports(util.List.of(new V1ContainerPort().containerPort(8080))))
          )
          .hostname(s"user-pod-$uid")
          .subdomain("workflow-pods")
          .overhead(null)
      )
    coreApi.createNamespacedPod(namespace, pod).execute()
  }

  /**
   * Deletes an existing pod belonging to the specific user id.
   *
   * @param uid   The pod owner's user id.
   */
  def deletePod(uid: Int): Unit = {
    coreApi.deleteNamespacedPod(s"user-pod-$uid", namespace).execute()
  }

  /**
   * Sends given workflow to specified user's pod and gets workflow response.
   *
   * @param uid        The user id who the workflow belongs to.
   * @param workflow   The JSON representation of a texera workflow.
   * @return The response object produced by the worker pod.
   */
  def sendWorkflow(uid: String, workflow: String): Map[String, String] = ???

  /**
   * Find and replace pod in case of pod failure.
   *
   * @param uid        The uid which a new pod will be created for.
   * @return A newly created pod.
   */
  private def pollForFailingPod(uid: Int): V1Pod = ???
}
