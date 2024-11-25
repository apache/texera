import config.ApplicationConf
import io.dropwizard.core.{Application, Configuration}
import io.dropwizard.core.setup.{Bootstrap, Environment}
import web.resources.{HelloWorldResource, WorkflowPodBrainResource}

class WorkflowPodBrainApplication extends Application[Configuration] {

  override def initialize(bootstrap: Bootstrap[Configuration]): Unit = {}

  override def run(configuration: Configuration, environment: Environment): Unit = {
    val appConfig = ApplicationConf.appConfig

    // Register http resources
    environment.jersey().register(new HelloWorldResource)
    environment.jersey().register(new WorkflowPodBrainResource)
  }
}

object WorkflowPodBrainApplication {
  def main(args: Array[String]): Unit = {
    new WorkflowPodBrainApplication().run(args: _*)
  }
}
