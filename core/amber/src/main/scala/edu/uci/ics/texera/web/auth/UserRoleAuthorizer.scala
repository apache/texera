package edu.uci.ics.texera.web.auth

import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import io.dropwizard.auth.Authorizer

object UserRoleAuthorizer extends Authorizer[SessionUser] {
  override def authorize(user: SessionUser, role: String): Boolean = {
    user.isRoleOf(UserRoleEnum.valueOf(role))
  }
}
