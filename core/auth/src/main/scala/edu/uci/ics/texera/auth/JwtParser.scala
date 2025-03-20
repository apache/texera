package edu.uci.ics.texera.auth

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import org.jose4j.jwt.JwtClaims
import org.jose4j.jwt.consumer.{JwtConsumer, JwtConsumerBuilder}
import org.jose4j.keys.HmacKey
import org.jose4j.lang.UnresolvableKeyException

import java.nio.charset.StandardCharsets
import java.util.Optional

object JwtParser extends LazyLogging {

  private val TOKEN_SECRET = AuthConfig.jwtSecretKey.toLowerCase() match {
    case "random" => getRandomHexString
    case _        => AuthConfig.jwtSecretKey
  }

  private val jwtConsumer: JwtConsumer = new JwtConsumerBuilder()
    .setAllowedClockSkewInSeconds(30)
    .setRequireExpirationTime()
    .setRequireSubject()
    .setVerificationKey(new HmacKey(TOKEN_SECRET.getBytes(StandardCharsets.UTF_8)))
    .setRelaxVerificationKeyValidation()
    .build()

  def parseToken(token: String): Optional[SessionUser] = {
    try {
      val jwtClaims: JwtClaims = jwtConsumer.processToClaims(token)
      val userName = jwtClaims.getSubject
      val email = jwtClaims.getClaimValue("email", classOf[String])
      val userId = jwtClaims.getClaimValue("userId").asInstanceOf[Long].toInt
      val role = UserRoleEnum.valueOf(jwtClaims.getClaimValue("role").asInstanceOf[String])
      val googleId = jwtClaims.getClaimValue("googleId", classOf[String])

      val user = new User(userId, userName, email, null, googleId, null, role)
      Optional.of(new SessionUser(user))
    } catch {
      case _: UnresolvableKeyException =>
        logger.error("Invalid JWT Signature")
        Optional.empty()
      case e: Exception =>
        logger.error(s"Failed to parse JWT: ${e.getMessage}")
        Optional.empty()
    }
  }

  private def getRandomHexString: String = {
    val bytes = 32
    val r = new scala.util.Random()
    val sb = new StringBuilder
    while (sb.length < bytes)
      sb.append(Integer.toHexString(r.nextInt()))
    sb.toString.substring(0, bytes)
  }
}
