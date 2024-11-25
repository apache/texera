package config

import com.typesafe.config.{Config, ConfigFactory}

case class KubernetesConfig(
                             namespace: String,
                             workflowPodBrainNamespace: String,
                             workflowPodPoolNamespace: String,
                             workflowPodBrainDeploymentName: String
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
      namespace = config.getString("kubernetes.namespace"),
      workflowPodBrainNamespace = config.getString("kubernetes.brain-namespace"),
      workflowPodPoolNamespace = config.getString("kubernetes.pool-namespace"),
      workflowPodBrainDeploymentName = config.getString("kubernetes.workflow-pod-brain-deployment-name")
    ),
    mysqlConfig = MysqlConfig(
      url = config.getString("jdbc.url"),
      username = config.getString("jdbc.username"),
      password = config.getString("jdbc.password")
    )
  )

  private def expandPath(path: String): String = {
    if (path.startsWith("~")) {
      System.getProperty("user.home") + path.substring(1)
    } else {
      path
    }
  }
}