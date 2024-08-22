package edu.uci.ics.texera.web

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.toastshaman.dropwizard.auth.jwt.JwtAuthFilter
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.TexeraWebApplication.parseArgs
import edu.uci.ics.texera.web.auth.JwtAuth.jwtConsumer
import edu.uci.ics.texera.web.auth.{
  GuestAuthFilter,
  SessionUser,
  UserAuthenticator,
  UserRoleAuthorizer
}
import edu.uci.ics.texera.web.resource.WorkflowCompilationResource
import io.dropwizard.auth.{AuthDynamicFeature, AuthValueFactoryProvider}
import io.dropwizard.setup.{Bootstrap, Environment}
import org.eclipse.jetty.server.session.SessionHandler
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature

object TexeraWorkflowCompilerService {
  def main(args: Array[String]): Unit = {
    val argMap = parseArgs(args)

    new TexeraWorkflowCompilerService().run(
      "server",
      Utils.amberHomePath
        .resolve("src")
        .resolve("main")
        .resolve("resources")
        .resolve("texera-compiler-web-config.yml")
        .toString
    )
  }
}

class TexeraWorkflowCompilerService
    extends io.dropwizard.Application[TexeraWorkflowCompilerWebServiceConfiguration]
    with LazyLogging {
  override def initialize(bootstrap: Bootstrap[TexeraWorkflowCompilerWebServiceConfiguration]): Unit = {
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }

  override def run(
                    configuration: TexeraWorkflowCompilerWebServiceConfiguration,
                    environment: Environment
  ): Unit = {
    // serve backend at /api/texera
    environment.jersey.setUrlPattern("/api/texera/*")

    // register the compilation endpoint
    environment.jersey.register(classOf[WorkflowCompilationResource])
  }
}
