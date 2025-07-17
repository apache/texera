package edu.uci.ics.texera.service

import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.config.StorageConfig
import edu.uci.ics.amber.util.PathUtils.{configServicePath, permissionServicePath}
import edu.uci.ics.texera.auth.{JwtAuthFilter, SessionUser}
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.service.resource.{HealthCheckResource, PermissionResource}
import io.dropwizard.auth.AuthDynamicFeature
import org.eclipse.jetty.server.session.SessionHandler
import org.jooq.impl.DSL


class PermissionService extends Application[PermissionServiceConfiguration] with LazyLogging {
  override def initialize(bootstrap: Bootstrap[PermissionServiceConfiguration]): Unit = {
    // Register Scala module to Dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)

    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )
  }

  override def run(configuration: PermissionServiceConfiguration, environment: Environment): Unit = {
    // Serve backend at /api
    environment.jersey.setUrlPattern("/api/*")

    environment.jersey.register(classOf[SessionHandler])
    environment.servlets.setSessionHandler(new SessionHandler)

    environment.jersey.register(classOf[HealthCheckResource])
    environment.jersey.register(classOf[PermissionResource])

    // Register JWT authentication filter
    environment.jersey.register(new AuthDynamicFeature(classOf[JwtAuthFilter]))

    // Enable @Auth annotation for injecting SessionUser
    environment.jersey.register(
      new io.dropwizard.auth.AuthValueFactoryProvider.Binder(classOf[SessionUser])
    )
  }
}
object PermissionService {
  def main(args: Array[String]): Unit = {
    val permissionPath = permissionServicePath
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("permission-service-web-config.yaml")
      .toAbsolutePath
      .toString

    // Start the Dropwizard application
    new PermissionService().run("server", permissionPath)
  }
}
