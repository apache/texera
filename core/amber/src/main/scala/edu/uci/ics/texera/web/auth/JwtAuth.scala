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
  val jwtConfig: Config = AmberUtils.amberConfig.getConfig("user-sys.jwt")
  val jwtTokenSecret: String = jwtConfig.getString("256-bit-secret").toLowerCase() match {
    case "random" => getRandomHexString
    case _        => jwtConfig.getString("256-bit-secret")
  }
  // create the JwtConsumer instance
  val jwtConsumer: JwtConsumer = new JwtConsumerBuilder()
    .setAllowedClockSkewInSeconds(30)
    .setRequireExpirationTime()
    .setRequireSubject()
    .setVerificationKey(new HmacKey(jwtTokenSecret.getBytes))
    .setRelaxVerificationKeyValidation()
    .build

  def generateNewJwtToken(claims: JwtClaims): String = {
    val jws = new JsonWebSignature()
    jws.setPayload(claims.toJson)
    jws.setAlgorithmHeaderValue(HMAC_SHA256)
    jws.setKey(new HmacKey(jwtTokenSecret.getBytes))
    jws.getCompactSerialization
  }

  def generateNewJwtClaims(user: User, expireInDays: Int): JwtClaims = {
    val claims = new JwtClaims
    claims.setSubject(user.getName)
    claims.setClaim("userId", user.getUid)
    claims.setExpirationTimeMinutesInTheFuture(expireInDays * 24 * 60)
    claims
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
