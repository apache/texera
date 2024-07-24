package service

import config.ApplicationConf.appConfig
import io.kubernetes.client.openapi.{ApiClient, Configuration}
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.{V1Pod, V1PodList}
import io.kubernetes.client.util.Config
import service.KubernetesClientConfig.kubernetesConfig

object KubernetesClientConfig {

  val kubernetesConfig = appConfig.kubernetes
  def createKubernetesClient(): CoreV1Api = {
    val client: ApiClient = Config.fromConfig(kubernetesConfig.kubeConfigPath)
    Configuration.setDefaultApiClient(client)
    new CoreV1Api(client)
  }
}

class KubernetesClientService(
                               val namespace: String = kubernetesConfig.namespace,
                               val deploymentName: String = kubernetesConfig.workflowPodPoolDeploymentName) {

  private val api: CoreV1Api = KubernetesClientConfig.createKubernetesClient()

  /**
    * Retrieves the list of pods for a given deployment in the specified namespace.
    *
    * @return A list of V1Pod objects representing the pods in the deployment.
    */
  def getPodsList(): List[V1Pod] = ???

  /**
    * Creates a new pod under the specified deployment.
    *
    * @param podSpec        The specification of the pod to be created.
    * @return The newly created V1Pod object.
    */
  def createPod(podSpec: V1Pod): V1Pod = ???

  /**
    * Deletes an existing pod in the specified namespace.
    *
    * @param podName   The name of the pod to be deleted.
    */
  def deletePod(podName: String): Unit = ???
}
