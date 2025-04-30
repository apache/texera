package edu.uci.ics.texera.service

import com.fasterxml.jackson.databind.module.SimpleModule
import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.util.PathUtils.fileServicePath
import edu.uci.ics.texera.auth.{JwtAuthFilter, SessionUser}
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.service.`type`.DatasetFileNode
import edu.uci.ics.texera.service.`type`.serde.DatasetFileNodeSerializer
import edu.uci.ics.texera.service.resource.{
  DatasetAccessResource,
  DatasetResource,
  HealthCheckResource
}
import edu.uci.ics.texera.service.util.LakeFSHealthManager
import io.dropwizard.auth.AuthDynamicFeature
import org.eclipse.jetty.server.session.SessionHandler

class FileService extends Application[FileServiceConfiguration] with LazyLogging {
  override def initialize(bootstrap: Bootstrap[FileServiceConfiguration]): Unit = {
    // Register Scala module to Dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)

    // register a new custom module just for DatasetFileNode serde/deserde
    val customSerializerModule = new SimpleModule("CustomSerializers")
    customSerializerModule.addSerializer(classOf[DatasetFileNode], new DatasetFileNodeSerializer())
    bootstrap.getObjectMapper.registerModule(customSerializerModule)
  }

  override def run(configuration: FileServiceConfiguration, environment: Environment): Unit = {
    // Serve backend at /api
    environment.jersey.setUrlPattern("/api/*")
    SqlServer.initConnection(
      StorageConfig.jdbcUrl,
      StorageConfig.jdbcUsername,
      StorageConfig.jdbcPassword
    )

    environment.jersey.register(classOf[SessionHandler])
    environment.servlets.setSessionHandler(new SessionHandler)

    environment.jersey.register(classOf[HealthCheckResource])

    // Register JWT authentication filter
    environment.jersey.register(new AuthDynamicFeature(classOf[JwtAuthFilter]))

    // Enable @Auth annotation for injecting SessionUser
    environment.jersey.register(
      new io.dropwizard.auth.AuthValueFactoryProvider.Binder(classOf[SessionUser])
    )

    environment.jersey.register(classOf[DatasetResource])
    environment.jersey.register(classOf[DatasetAccessResource])

    // Add health check for underlying S3 & LakeFS storage
    val healthCheckIntervalSeconds = 60
    environment.lifecycle().manage(new LakeFSHealthManager(healthCheckIntervalSeconds))
  }
}

object FileService {
  def main(args: Array[String]): Unit = {
    // Set the configuration file's path
    val configFilePath = fileServicePath
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("file-service-web-config.yaml")
      .toAbsolutePath
      .toString

    // Start the Dropwizard application
    new FileService().run("server", configFilePath)
  }
}
