package edu.uci.ics.texera.web.auth

import edu.uci.ics.texera.config.AuthConfig
import org.jose4j.jwt.JwtClaims
import org.jose4j.jwt.consumer.{JwtConsumer, JwtConsumerBuilder}
import org.jose4j.keys.HmacKey
import org.jose4j.lang.UnresolvableKeyException

import java.nio.charset.StandardCharsets
import javax.ws.rs.NotAuthorizedException

object DownloadTokenAuthenticator {

  private val TOKEN_SECRET = AuthConfig.jwtSecretKey

  private val jwtConsumer: JwtConsumer = new JwtConsumerBuilder()
    .setAllowedClockSkewInSeconds(30)
    .setRequireExpirationTime()
    .setRequireSubject()
    .setVerificationKey(new HmacKey(TOKEN_SECRET.getBytes(StandardCharsets.UTF_8)))
    .setRelaxVerificationKeyValidation()
    .build()

  def parseToken(token: String): DownloadTokenClaims = {
    try {
      val jwtClaims: JwtClaims = jwtConsumer.processToClaims(token)
      val exportType = jwtClaims.getClaimValue("exportType", classOf[String])
      val workflowId = jwtClaims.getClaimValue("workflowId").asInstanceOf[Long].toInt
      val workflowName = jwtClaims.getClaimValue("workflowName", classOf[String])
      val rowIndex = jwtClaims.getClaimValue("rowIndex").asInstanceOf[Long].toInt
      val columnIndex = jwtClaims.getClaimValue("columnIndex").asInstanceOf[Long].toInt
      val filename = jwtClaims.getClaimValue("filename", classOf[String])
      val computingUnitId = jwtClaims.getClaimValue("computingUnitId").asInstanceOf[Long].toInt
      val destination = jwtClaims.getClaimValue("destination", classOf[String])

      DownloadTokenClaims(
        exportType,
        workflowId,
        workflowName,
        rowIndex,
        columnIndex,
        filename,
        computingUnitId,
        destination
      )

    } catch {
      case _: UnresolvableKeyException =>
        throw new NotAuthorizedException("Invalid JWT Signature")
      case e: Exception =>
        throw new NotAuthorizedException(s"Failed to parse JWT: ${e.getMessage}")
    }
  }

}
