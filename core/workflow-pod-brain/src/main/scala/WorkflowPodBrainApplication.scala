import config.ApplicationConf
import io.dropwizard.core.{Application, Configuration}
import io.dropwizard.core.setup.{Bootstrap, Environment}
import web.resources.{HelloWorldResource, WorkflowPodBrainResource}

class WorkflowPodBrainApplication extends Application[Configuration] {
  override def initialize(bootstrap: Bootstrap[Configuration]): Unit = {}

  override def run(configuration: Configuration, environment: Environment): Unit = {
    val appConfig = ApplicationConf.appConfig

    environment.jersey().register(new HelloWorldResource)
    environment.jersey().register(new WorkflowPodBrainResource)

    println(s"Kube Config Path: ${appConfig.kubernetes.kubeConfigPath}")
    println(s"Namespace: ${appConfig.kubernetes.namespace}")
    println(s"Workflow Pod Brain Deployment Name: ${appConfig.kubernetes.workflowPodBrainDeploymentName}")
    println(s"Workflow Pod Pool Deployment Name: ${appConfig.kubernetes.workflowPodPoolDeploymentName}")
  }
}

object WorkflowPodBrainApplication {
  def main(args: Array[String]): Unit = {
    new WorkflowPodBrainApplication().run(args: _*)
  }
}