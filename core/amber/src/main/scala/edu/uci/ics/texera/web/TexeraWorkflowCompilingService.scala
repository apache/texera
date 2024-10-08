package edu.uci.ics.texera.web

import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.github.toastshaman.dropwizard.auth.jwt.JwtAuthFilter
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.Utils
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
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature

object TexeraWorkflowCompilingService {
  def main(args: Array[String]): Unit = {

    new TexeraWorkflowCompilingService().run(
      "server",
      Utils.amberHomePath
        .resolve("src")
        .resolve("main")
        .resolve("resources")
        .resolve("texera-compiling-service-web-config.yml")
        .toString
    )
  }
}

class TexeraWorkflowCompilingService
    extends io.dropwizard.Application[TexeraWorkflowCompilingServiceConfiguration]
    with LazyLogging {
  override def initialize(
      bootstrap: Bootstrap[TexeraWorkflowCompilingServiceConfiguration]
  ): Unit = {
    // register scala module to dropwizard default object mapper
    bootstrap.getObjectMapper.registerModule(DefaultScalaModule)
  }

  override def run(
      configuration: TexeraWorkflowCompilingServiceConfiguration,
      environment: Environment
  ): Unit = {
    // serve backend at /api/texera
    environment.jersey.setUrlPattern("/api/texera/*")

    // register the compilation endpoint
    environment.jersey.register(classOf[WorkflowCompilationResource])

    // Add JWT Auth layer (without session)
    if (AmberConfig.isUserSystemEnabled) {
      environment.jersey.register(
        new AuthDynamicFeature(
          new JwtAuthFilter.Builder[SessionUser]()
            .setJwtConsumer(jwtConsumer)
            .setRealm("realm")
            .setPrefix("Bearer")
            .setAuthenticator(UserAuthenticator)
            .setAuthorizer(UserRoleAuthorizer)
            .buildAuthFilter()
        )
      )
    } else {
      environment.jersey.register(
        new AuthDynamicFeature(
          new GuestAuthFilter.Builder().setAuthorizer(UserRoleAuthorizer).buildAuthFilter()
        )
      )
    }

    environment.jersey.register(
      new AuthValueFactoryProvider.Binder[SessionUser](classOf[SessionUser])
    )
    environment.jersey.register(classOf[RolesAllowedDynamicFeature])
  }
}
