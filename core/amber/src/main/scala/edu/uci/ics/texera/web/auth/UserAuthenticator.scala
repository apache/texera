package edu.uci.ics.texera.web.auth

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import io.dropwizard.auth.Authenticator
import org.jooq.types.UInteger
import org.jose4j.jwt.consumer.JwtContext

import java.util.Optional

object UserAuthenticator extends Authenticator[JwtContext, SessionUser] with LazyLogging {
  override def authenticate(context: JwtContext): Optional[SessionUser] = {
    // This method will be called once the token's signature has been verified,
    // including the token secret and the expiration time
    try {
      val userName = context.getJwtClaims.getSubject
      val userId = UInteger.valueOf(context.getJwtClaims.getClaimValue("userId").asInstanceOf[Long])
      val user = new User(userName, userId, null, null)
      Optional.of(new SessionUser(user, Set(SessionRole.BASIC)))
    } catch {
      case e: Exception =>
        logger.error("Failed to authenticate the JwtContext", e)
        Optional.empty()
    }

  }
}
