package edu.uci.ics.texera.web.auth

import edu.uci.ics.texera.web.model.jooq.generated.enums.UserRole
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User

import java.security.Principal

class SessionUser(val user: User) extends Principal {
  def getUser: User = user

  override def getName: String = user.getName

  def isRoleOf(role: UserRole): Boolean = user.getRole == role
}
