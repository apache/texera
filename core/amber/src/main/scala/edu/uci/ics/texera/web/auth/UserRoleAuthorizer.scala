package edu.uci.ics.texera.web.auth

import edu.uci.ics.texera.web.model.jooq.generated.enums.UserRole
import io.dropwizard.auth.Authorizer

object UserRoleAuthorizer extends Authorizer[SessionUser] {
  override def authorize(user: SessionUser, role: String): Boolean = {
    user.isRoleOf(UserRole.valueOf(role))
  }
}
