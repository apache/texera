package edu.uci.ics.texera.web.auth

import edu.uci.ics.texera.dao.jooq.generated.enums.UserRole
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import org.jooq.types.UInteger

import java.security.Principal

class SessionUser(val user: User) extends Principal {
  def getUser: User = user

  override def getName: String = user.getName

  def getUid: UInteger = user.getUid

  def getEmail: String = user.getEmail

  def getGoogleId: String = user.getGoogleId

  def isRoleOf(role: UserRole): Boolean = user.getRole == role
}
