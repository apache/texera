package edu.uci.ics.texera.web.auth

import com.github.toastshaman.dropwizard.auth.jwt.JwtAuthFilter
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.auth.JwtAuth.jwtConsumer
import edu.uci.ics.texera.auth.SessionUser
import io.dropwizard.auth.AuthDynamicFeature
import io.dropwizard.setup.Environment

// TODO: move this logic to Auth
@Deprecated
object JwtAuth {
  def setupJwtAuth(environment: Environment): Unit = {
    if (AmberConfig.isUserSystemEnabled) {
      // register JWT Auth layer
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
      // register Guest Auth layer
      environment.jersey.register(
        new AuthDynamicFeature(
          new GuestAuthFilter.Builder().setAuthorizer(UserRoleAuthorizer).buildAuthFilter()
        )
      )
    }
  }
}
