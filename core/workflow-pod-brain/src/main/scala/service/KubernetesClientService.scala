package service

import config.ApplicationConf.appConfig
import io.kubernetes.client.openapi.{ApiClient, Configuration}
import io.kubernetes.client.openapi.apis.{AppsV1Api, CoreV1Api}
import io.kubernetes.client.openapi.models.{V1Container, V1ContainerPort, V1ObjectMeta, V1Pod, V1PodList, V1PodSpec}
import io.kubernetes.client.util.Config
import service.KubernetesClientConfig.kubernetesConfig

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala
import sttp.client4.quick.{RichRequest, quickRequest}
import sttp.client4.{Response, UriContext}
import sttp.model.Uri
import sttp.model.Uri.QuerySegment.Value

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
                               val brainNamespace: String = kubernetesConfig.workflowPodBrainNamespace,
                               val poolNamespace: String = kubernetesConfig.workflowPodPoolNamespace,
                               val deploymentName: String = kubernetesConfig.workflowPodBrainDeploymentName) {

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
  def getPodsList(namespace: String): List[V1Pod] = {
    coreApi.listNamespacedPod(namespace).execute().getItems.asScala.toList
  }

  /**
   * Retrieves the list of pods for a given label in the specified namespace.
   *
   * @param podLabel        The label of the pods to be returned.
   * @return A list of V1Pod objects representing the pods with the given label.
   */
  def getPodsList(namespace: String, podLabel: String): List[V1Pod] = {
    coreApi.listNamespacedPod(namespace).labelSelector(podLabel).execute().getItems.asScala.toList
  }

  /**
   * Retrieves a single with the given label in the specified namespace.
   *
   * @param podLabel        The label of the pods to be returned.
   * @return A list of V1Pod objects representing the pods with the given label.
   */
  def getPodFromLabel(namespace: String, podLabel: String): V1Pod = {
    coreApi.listNamespacedPod(namespace).labelSelector(podLabel).limit(1).execute().getItems.asScala.toList.last
  }

  /**
   * Creates a new pod under the specified namespace.
   *
   * @param uid        The uid which a new pod will be created for.
   * @return The newly created V1Pod object.
   */
  def createPod(uid: Int): V1Pod = {
    val uidString: String = String.valueOf(uid)
    val uidLabelSelector: String = s"userId=$uidString"
    val pod: V1Pod = createUserPod(uid)

    // Should be a list with a single pod
    try {
      getPodFromLabel(poolNamespace, uidLabelSelector)
    }
    catch {
      // In case program moves too fast and newly created pod is not detectable yet
      case e: java.util.NoSuchElementException =>
        println(e.getMessage)
        Thread.sleep(1000)
        println("Attempting to retrieve pod again")
        getPodFromLabel(poolNamespace, uidLabelSelector)
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
          .namespace(poolNamespace)
          .labels(util.Map.of("userId", uidString, "workflow", "worker"))
      )
      .spec(
        new V1PodSpec()
          .containers(
            util.List.of(new V1Container()
              .name("worker")
              .image("ksdn117/web-socket-test")
              .ports(util.List.of(new V1ContainerPort().containerPort(8010))))
          )
          .hostname(s"user-pod-$uid")
          .subdomain("workflow-pods")
          .overhead(null)
      )
    coreApi.createNamespacedPod(poolNamespace, pod).execute()
  }

  /**
   * Deletes an existing pod belonging to the specific user id.
   *
   * @param uid   The pod owner's user id.
   */
  def deletePod(uid: Int): Unit = {
    coreApi.deleteNamespacedPod(s"user-pod-$uid", poolNamespace).execute()
  }

  /**
   * Find and replace pod in case of pod failure.
   *
   * @param uid        The uid which a new pod will be created for.
   * @return A newly created pod.
   */
  private def pollForFailingPod(uid: Int): V1Pod = ???
}
