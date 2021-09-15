package edu.uci.ics.texera.web.basicauth

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.USER
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import io.dropwizard.auth.Authenticator
import org.jose4j.jwt.MalformedClaimException
import org.jose4j.jwt.consumer.JwtContext

import java.util.Optional

object AppAuthenticator extends Authenticator[JwtContext, SessionUser] {
  override def authenticate(context: JwtContext): Optional[SessionUser] = {
    // This method will be called once the token's signature has been verified
    try {

      // TODO: verify that the provided token has not expired.
      val userName = context.getJwtClaims.getSubject

      Optional
        .of(
          SqlServer.createDSLContext
            .select()
            .from(USER)
            .where(USER.NAME.eq(userName).and(USER.GOOGLE_ID.isNull))
            .fetchOneInto(classOf[User])
        )
        .map(u => new SessionUser(u))

    } catch {
      case _: MalformedClaimException =>
        Optional.empty()
    }
// All JsonWebTokenExceptions will result in a 401 Unauthorized response.
  }
}
