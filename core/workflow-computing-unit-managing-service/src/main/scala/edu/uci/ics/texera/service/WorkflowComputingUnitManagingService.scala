package edu.uci.ics.texera.service

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.uci.ics.amber.util.PathUtils.workflowComputingUnitManagingServicePath
import edu.uci.ics.texera.service.resource.WorkflowComputingUnitManagingResource
import io.dropwizard.core.setup.{Bootstrap, Environment}
import io.dropwizard.core.Application

class WorkflowComputingUnitManagingService
    extends Application[WorkflowComputingUnitManagingServiceConfiguration] {

  override def initialize(
      bootstrap: Bootstrap[WorkflowComputingUnitManagingServiceConfiguration]
  ): Unit = {
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }
  override def run(
      configuration: WorkflowComputingUnitManagingServiceConfiguration,
      environment: Environment
  ): Unit = {
    // Register http resources
    environment.jersey.setUrlPattern("/api/*")
    environment.jersey().register(new WorkflowComputingUnitManagingResource)
  }
}

object WorkflowComputingUnitManagingService {
  def main(args: Array[String]): Unit = {
    val configFilePath = workflowComputingUnitManagingServicePath
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("workflow-computing-unit-managing-service-config.yaml")
      .toAbsolutePath
      .toString

    new WorkflowComputingUnitManagingService().run("server", configFilePath)
  }
}
