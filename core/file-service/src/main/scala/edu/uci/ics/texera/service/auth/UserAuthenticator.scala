package edu.uci.ics.texera.service.auth

import edu.uci.ics.texera.dao.jooq.generated.enums.UserRole
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import io.dropwizard.auth.Authenticator
import org.jooq.types.UInteger

import java.util.Optional
import org.jose4j.jwt.consumer.JwtContext

class UserAuthenticator extends Authenticator[JwtContext, SessionUser] {
  override def authenticate(context: JwtContext): Optional[SessionUser] = {
    try {
      val jwtClaims = context.getJwtClaims

      val userName = jwtClaims.getSubject
      val email = jwtClaims.getClaimValue("email", classOf[String])
      val userId = UInteger.valueOf(jwtClaims.getClaimValue("userId").asInstanceOf[Long])
      val role = UserRole.valueOf(jwtClaims.getClaimValue("role").asInstanceOf[String])
      val googleId = jwtClaims.getClaimValue("googleId", classOf[String])

      // Construct a SessionUser object
      val user = new SessionUser(
        new User(userId, userName, email, null, googleId, role, null)
      )
      Optional.of(user)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Optional.empty()
    }
  }
}
