package edu.uci.ics.texera.web.auth

import com.typesafe.config.Config
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.User
import org.jose4j.jws.AlgorithmIdentifiers.HMAC_SHA256
import org.jose4j.jws.JsonWebSignature
import org.jose4j.jwt.JwtClaims
import org.jose4j.jwt.consumer.{JwtConsumer, JwtConsumerBuilder}
import org.jose4j.keys.HmacKey

import java.util.Random

object JwtAuth {

  final val jwtConfig: Config = AmberUtils.amberConfig.getConfig("user-sys.jwt")
  final val TOKEN_EXPIRE_TIME_IN_DAYS = jwtConfig.getString("exp-in-days").toInt
  final val TOKEN_SECRET: String = jwtConfig.getString("256-bit-secret").toLowerCase() match {
    case "random" => getRandomHexString
    case _        => jwtConfig.getString("256-bit-secret")
  }

  val jwtConsumer: JwtConsumer = new JwtConsumerBuilder()
    .setAllowedClockSkewInSeconds(30)
    .setRequireExpirationTime()
    .setRequireSubject()
    .setVerificationKey(new HmacKey(TOKEN_SECRET.getBytes))
    .setRelaxVerificationKeyValidation()
    .build

  def jwtToken(claims: JwtClaims): String = {
    val jws = new JsonWebSignature()
    jws.setPayload(claims.toJson)
    jws.setAlgorithmHeaderValue(HMAC_SHA256)
    jws.setKey(new HmacKey(TOKEN_SECRET.getBytes))
    jws.getCompactSerialization
  }

  def jwtClaims(user: User, expireInDays: Int): JwtClaims = {
    val claims = new JwtClaims
    claims.setSubject(user.getName)
    claims.setClaim("userId", user.getUid)
    claims.setClaim("googleId", user.getGoogleId)
    claims.setExpirationTimeMinutesInTheFuture(dayToMin(expireInDays))
    claims
  }

  def dayToMin(days: Int): Int = {
    days * 24 * 60
  }

  private def getRandomHexString: String = {
    val bytes = 32
    val r = new Random()
    val sb = new StringBuffer
    while (sb.length < bytes)
      sb.append(Integer.toHexString(r.nextInt()))
    sb.toString.substring(0, bytes)
  }
}
