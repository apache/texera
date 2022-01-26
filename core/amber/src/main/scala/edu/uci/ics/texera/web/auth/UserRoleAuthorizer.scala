package edu.uci.ics.texera.web.auth

import io.dropwizard.auth.Authorizer

object UserRoleAuthorizer extends Authorizer[SessionUser] {
  override def authorize(user: SessionUser, role: String): Boolean = {
    user.isRoleOf(SessionRole.valueOf(role))
  }
}
