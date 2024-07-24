package config

import com.typesafe.config.{Config, ConfigFactory}

case class KubernetesConfig(
                             kubeConfigPath: String,
                             namespace: String,
                             workflowPodBrainDeploymentName: String,
                             workflowPodPoolDeploymentName: String
                           )

case class AppConfig(
                      kubernetes: KubernetesConfig
                    )

object ApplicationConf {
  private val config: Config = ConfigFactory.load()

  val appConfig: AppConfig = AppConfig(
    kubernetes = KubernetesConfig(
      kubeConfigPath = config.getString("kubernetes.kube-config-path"),
      namespace = config.getString("kubernetes.namespace"),
      workflowPodBrainDeploymentName = config.getString("kubernetes.workflow-pod-brain-deployment-name"),
      workflowPodPoolDeploymentName = config.getString("kubernetes.workflow-pod-pool-deployment-name")
    )
  )
}