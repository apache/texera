package edu.uci.ics.texera.service.auth

import edu.uci.ics.texera.dao.jooq.generated.enums.UserRole
import io.dropwizard.auth.Authorizer
import jakarta.ws.rs.container.ContainerRequestContext

object UserRoleAuthorizer extends Authorizer[SessionUser] {
  override def authorize(
      user: SessionUser,
      role: String,
      requestContext: ContainerRequestContext
  ): Boolean = {
    user.isRoleOf(UserRole.valueOf(role))
  }
}
