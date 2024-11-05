package edu.uci.ics.amber.service

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.uci.ics.amber.compiler.util.PathUtil.webConfigFilePath
import edu.uci.ics.amber.service.resource.WorkflowCompilationResource
import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}

class WorkflowCompilingService extends Application[WorkflowCompilingServiceConfiguration] {
  override def initialize(bootstrap: Bootstrap[WorkflowCompilingServiceConfiguration]): Unit = {
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }

  override def run(configuration: WorkflowCompilingServiceConfiguration, environment: Environment): Unit = {
    // serve backend at /api/texera
    environment.jersey.setUrlPattern("/api/texera/*")

    // register the compilation endpoint
    environment.jersey.register(classOf[WorkflowCompilationResource])
  }
}

object WorkflowCompilingService {
  def main(args: Array[String]): Unit = {
    // Check if a configuration file path is passed; use a default if none is provided
    val configFilePath = webConfigFilePath.toAbsolutePath.toString

    // Start the Dropwizard application with the specified configuration file path
    new WorkflowCompilingService().run("server", configFilePath)
  }
}
