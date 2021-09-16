package edu.uci.ics.texera.web.basicauth

import org.jose4j.jwt.consumer.{JwtConsumer, JwtConsumerBuilder}
import org.jose4j.keys.HmacKey

object JwtAuth {
    val jwtTokenSecret: String = "dfwzsdzwh823zebdwdz772632gdsbd21"
    // create the JwtConsumer instance
    val jwtConsumer: JwtConsumer = new JwtConsumerBuilder()
        .setAllowedClockSkewInSeconds(30)
        .setRequireExpirationTime()
        .setRequireSubject()
        .setVerificationKey(new HmacKey(jwtTokenSecret.getBytes))
        .setRelaxVerificationKeyValidation()
        .build
}
