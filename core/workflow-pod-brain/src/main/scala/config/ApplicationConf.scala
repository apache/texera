package config

import com.typesafe.config.{Config, ConfigFactory}

case class KubernetesConfig(
                             kubeConfigPath: String,
                             namespace: String,
                             workflowPodBrainDeploymentName: String,
                             workflowPodPoolDeploymentName: String
                           )

case class MysqlConfig(
    url: String,
    username: String,
    password: String,
                      )

case class AppConfig(
                      kubernetes: KubernetesConfig,
                      mysqlConfig: MysqlConfig
                    )

object ApplicationConf {
  private val config: Config = ConfigFactory.load()

  val appConfig: AppConfig = AppConfig(
    kubernetes = KubernetesConfig(
      kubeConfigPath = config.getString("kubernetes.kube-config-path"),
      namespace = config.getString("kubernetes.namespace"),
      workflowPodBrainDeploymentName = config.getString("kubernetes.workflow-pod-brain-deployment-name"),
      workflowPodPoolDeploymentName = config.getString("kubernetes.workflow-pod-pool-deployment-name")
    ),
    mysqlConfig = MysqlConfig(
      url = config.getString("jdbc.url"),
      username = config.getString("jdbc.username"),
      password = config.getString("jdbc.password")
    )
  )
}