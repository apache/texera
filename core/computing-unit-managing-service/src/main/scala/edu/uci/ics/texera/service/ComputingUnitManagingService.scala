package edu.uci.ics.texera.service

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.util.PathUtils.workflowComputingUnitManagingServicePath
import edu.uci.ics.texera.auth.{JwtAuthFilter, SessionUser}
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.service.resource.{ComputingUnitManagingResource, HealthCheckResource}
import io.dropwizard.auth.AuthDynamicFeature
import io.dropwizard.core.setup.{Bootstrap, Environment}
import io.dropwizard.core.Application

class ComputingUnitManagingService extends Application[ComputingUnitManagingServiceConfiguration] {

  override def initialize(
      bootstrap: Bootstrap[ComputingUnitManagingServiceConfiguration]
  ): Unit = {
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }
  override def run(
      configuration: ComputingUnitManagingServiceConfiguration,
      environment: Environment
  ): Unit = {
    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )
    // Register http resources
    environment.jersey.setUrlPattern("/api/*")
    environment.jersey.register(classOf[HealthCheckResource])

    // Register JWT authentication filter
    environment.jersey.register(new AuthDynamicFeature(classOf[JwtAuthFilter]))

    // Enable @Auth annotation for injecting SessionUser
    environment.jersey.register(
      new io.dropwizard.auth.AuthValueFactoryProvider.Binder(classOf[SessionUser])
    )

    environment.jersey().register(new ComputingUnitManagingResource)
  }
}

object ComputingUnitManagingService {
  def main(args: Array[String]): Unit = {
    val configFilePath = workflowComputingUnitManagingServicePath
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("computing-unit-managing-service-config.yaml")
      .toAbsolutePath
      .toString

    new ComputingUnitManagingService().run("server", configFilePath)
  }
}
