import be.tomcools.dropwizard.websocket.WebsocketBundle
import config.ApplicationConf
import io.dropwizard.core.{Application, Configuration}
import io.dropwizard.core.setup.{Bootstrap, Environment}
import web.resources.{HelloWorldResource, WebsocketProxyEndpoint, WorkflowPodBrainResource}

class WorkflowPodBrainApplication extends Application[Configuration] {
  var websocket = new WebsocketBundle[Configuration]

  override def initialize(bootstrap: Bootstrap[Configuration]): Unit = {
    // Add WebsocketBundle to dropwizard
    bootstrap.addBundle(websocket)
  }

  override def run(configuration: Configuration, environment: Environment): Unit = {
    val appConfig = ApplicationConf.appConfig

    // Add any websocket endpoint here
    websocket.addEndpoint(classOf[WebsocketProxyEndpoint])

    // Register http resources
    environment.jersey().register(new HelloWorldResource)
    environment.jersey().register(new WorkflowPodBrainResource)

    println(s"Kube Config Path: ${appConfig.kubernetes.kubeConfigPath}")
  }
}

object WorkflowPodBrainApplication {
  def main(args: Array[String]): Unit = {
    new WorkflowPodBrainApplication().run(args: _*)
  }
}