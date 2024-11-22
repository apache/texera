package edu.uci.ics.texera.service

import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.uci.ics.amber.util.PathUtils.workflowCompilingServicePath
import edu.uci.ics.texera.service.resource.WorkflowCompilationResource

class WorkflowCompilingService extends Application[WorkflowCompilingServiceConfiguration] {
  override def initialize(bootstrap: Bootstrap[WorkflowCompilingServiceConfiguration]): Unit = {
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }

  override def run(
      configuration: WorkflowCompilingServiceConfiguration,
      environment: Environment
  ): Unit = {
    // serve backend at /api
    environment.jersey.setUrlPattern("/api/*")
    // register the compilation endpoint
    environment.jersey.register(classOf[WorkflowCompilationResource])
  }
}

object WorkflowCompilingService {
  def main(args: Array[String]): Unit = {
    // set the configuration file's path
    val configFilePath = workflowCompilingServicePath
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("workflow-compiling-service-config.yaml")
      .toAbsolutePath
      .toString

    // Start the Dropwizard application
    new WorkflowCompilingService().run("server", configFilePath)
  }
}
