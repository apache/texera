package edu.uci.ics.texera.web.auth

import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User

import java.security.Principal

class SessionUser(val user: User, val roles: Set[SessionRole]) extends Principal {
  def getUser: User = user

  override def getName: String = user.getName

  def isRoleOf(sessionRole: SessionRole): Boolean = roles.contains(sessionRole)
}
