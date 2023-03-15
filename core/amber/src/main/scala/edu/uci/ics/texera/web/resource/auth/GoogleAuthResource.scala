package edu.uci.ics.texera.web.resource.auth
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.typesafe.config.ConfigFactory
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.JwtAuth.{
  TOKEN_EXPIRE_TIME_IN_DAYS,
  dayToMin,
  jwtClaims,
  jwtToken
}
import edu.uci.ics.texera.web.model.http.response.TokenIssueResponse
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserRole
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.resource.auth.GoogleAuthResource.{userDao, verifier}
import java.util.Collections
import javax.ws.rs._
import javax.ws.rs.core.MediaType
object GoogleAuthResource {
  final private val userDao = new UserDao(SqlServer.createDSLContext.configuration)
  private val verifier = new GoogleIdTokenVerifier.Builder(new NetHttpTransport, new JacksonFactory)
    .setAudience(
      Collections.singletonList(ConfigFactory.load("google_api").getString("google.clientId"))
    )
    .build()
}

@Path("/auth/google")
class GoogleAuthResource {
  @POST
  @Consumes(Array(MediaType.TEXT_PLAIN))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/login")
  def login(credential: String): TokenIssueResponse = {
    val idToken = verifier.verify(credential)
    if (idToken != null) {
      val payload = idToken.getPayload
      val googleId = payload.getSubject
      val googleName = payload.get("name").asInstanceOf[String]
      val googleEmail = payload.getEmail
      val user = Option(userDao.fetchOneByGoogleId(googleId)) match {
        case Some(user) =>
          if (user.getName != googleName) {
            user.setName(googleName)
            userDao.update(user)
          }
          if (user.getEmail != googleEmail) {
            user.setEmail(googleEmail)
            userDao.update(user)
          }
          user
        case None =>
          Option(userDao.fetchOneByEmail(googleEmail)) match {
            case Some(user) =>
              if (user.getName != googleName) {
                user.setName(googleName)
              }
              user.setGoogleId(googleId)
              userDao.update(user)
              user
            case None =>
              // create a new user with googleId
              val user = new User
              user.setName(googleName)
              user.setEmail(googleEmail)
              user.setGoogleId(googleId)
              user.setRole(UserRole.INACTIVE)
              userDao.insert(user)
              user
          }
      }
      TokenIssueResponse(jwtToken(jwtClaims(user, dayToMin(TOKEN_EXPIRE_TIME_IN_DAYS))))
    } else throw new NotAuthorizedException("Login credentials are incorrect.")
  }
}
