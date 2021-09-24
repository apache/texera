package edu.uci.ics.texera.web.auth

import edu.uci.ics.amber.engine.common.AmberUtils
import org.jose4j.jwt.consumer.{JwtConsumer, JwtConsumerBuilder}
import org.jose4j.keys.HmacKey

object JwtAuth {
  val jwtTokenSecret: String = AmberUtils.amberConfig.getString("user-sys.jwt.256-bit-secret")
  // create the JwtConsumer instance
  val jwtConsumer: JwtConsumer = new JwtConsumerBuilder()
    .setAllowedClockSkewInSeconds(30)
    .setRequireExpirationTime()
    .setRequireSubject()
    .setVerificationKey(new HmacKey(jwtTokenSecret.getBytes))
    .setRelaxVerificationKeyValidation()
    .build
}
