package edu.uci.ics.texera.auth

import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import org.jose4j.jws.AlgorithmIdentifiers.HMAC_SHA256
import org.jose4j.jws.JsonWebSignature
import org.jose4j.jwt.JwtClaims
import org.jose4j.jwt.consumer.{JwtConsumer, JwtConsumerBuilder}
import org.jose4j.keys.HmacKey

import java.nio.charset.StandardCharsets

// TODO: move this logic to Auth
object JwtAuth {

  final val TOKEN_EXPIRE_TIME_IN_DAYS = AuthConfig.jwtExpirationDays
  final val TOKEN_SECRET: String = AuthConfig.jwtSecretKey

  val jwtConsumer: JwtConsumer = new JwtConsumerBuilder()
    .setAllowedClockSkewInSeconds(30)
    .setRequireExpirationTime()
    .setRequireSubject()
    .setVerificationKey(new HmacKey(TOKEN_SECRET.getBytes(StandardCharsets.UTF_8)))
    .setRelaxVerificationKeyValidation()
    .build

  def jwtToken(claims: JwtClaims): String = {
    val jws = new JsonWebSignature()
    jws.setPayload(claims.toJson)
    jws.setAlgorithmHeaderValue(HMAC_SHA256)
    jws.setKey(new HmacKey(TOKEN_SECRET.getBytes(StandardCharsets.UTF_8)))
    jws.getCompactSerialization
  }

  def jwtClaims(user: User, expireInDays: Int): JwtClaims = {
    val claims = new JwtClaims
    claims.setSubject(user.getName)
    claims.setClaim("userId", user.getUid)
    claims.setClaim("googleId", user.getGoogleId)
    claims.setClaim("email", user.getEmail)
    claims.setClaim("role", user.getRole)
    claims.setClaim("googleAvatar", user.getGoogleAvatar)
    claims.setExpirationTimeMinutesInTheFuture(dayToMin(expireInDays).toFloat)
    claims
  }

  def dayToMin(days: Int): Int = {
    days * 24 * 60
  }
}
