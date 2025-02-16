package edu.uci.ics.texera.service

import io.dropwizard.core.Application
import io.dropwizard.core.setup.{Bootstrap, Environment}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.uci.ics.amber.util.PathUtils.fileServicePath
import edu.uci.ics.texera.service.auth.JwtAuth
import edu.uci.ics.texera.service.resource.{DatasetAccessResource, DatasetResource}
import org.eclipse.jetty.server.session.SessionHandler
import org.glassfish.jersey.media.multipart.MultiPartFeature

class FileService extends Application[FileServiceConfiguration] {
  override def initialize(bootstrap: Bootstrap[FileServiceConfiguration]): Unit = {
    // Register Scala module to Dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }

  override def run(configuration: FileServiceConfiguration, environment: Environment): Unit = {
    // Serve backend at /api
    environment.jersey.setUrlPattern("/api/*")
    environment.jersey.register(classOf[SessionHandler])
    environment.servlets.setSessionHandler(new SessionHandler)
//    environment.jersey.register(classOf[MultiPartFeature])
    // Register JWT authentication
    JwtAuth.setupJwtAuth(environment)

    // Register multipart feature for file uploads
    environment.jersey.register(classOf[DatasetResource])
    environment.jersey.register(classOf[DatasetAccessResource])
  }
}

object FileService {
  def main(args: Array[String]): Unit = {
    // Set the configuration file's path
    val configFilePath = fileServicePath
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("file-service-config.yaml")
      .toAbsolutePath
      .toString

    // Start the Dropwizard application
    new FileService().run("server", configFilePath)
  }
}
