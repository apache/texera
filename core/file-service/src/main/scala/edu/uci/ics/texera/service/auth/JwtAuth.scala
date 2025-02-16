package edu.uci.ics.texera.service.auth

import com.github.toastshaman.dropwizard.auth.jwt.JwtAuthFilter
import io.dropwizard.auth.AuthDynamicFeature
import io.dropwizard.auth.AuthValueFactoryProvider
import io.dropwizard.auth.PrincipalImpl
import io.dropwizard.core.setup.Environment
import org.jose4j.jwt.consumer.{JwtConsumer, JwtConsumerBuilder}
import org.jose4j.keys.HmacKey

import java.util.Random

object JwtAuth {
  private val TOKEN_SECRET: String = AuthConfig.jwtSecretKey.toLowerCase() match {
    case "random" => getRandomHexString
    case _        => AuthConfig.jwtSecretKey
  }
  private val TOKEN_EXPIRATION_DAYS = AuthConfig.jwtExpirationDays

  // JWT Consumer for verification
  private val jwtConsumer: JwtConsumer = new JwtConsumerBuilder()
    .setAllowedClockSkewInSeconds(30)
    .setRequireExpirationTime()
    .setRequireSubject()
    .setVerificationKey(new HmacKey(TOKEN_SECRET.getBytes))
    .setRelaxVerificationKeyValidation()
    .build()

  // Register authentication middleware in Dropwizard
  def setupJwtAuth(environment: Environment): Unit = {
    environment.jersey.register(
      new AuthDynamicFeature(
        new JwtAuthFilter.Builder[SessionUser]()
          .setJwtConsumer(jwtConsumer)
          .setRealm("realm")
          .setPrefix("Bearer")
          .setAuthenticator(UserAuthenticator)
          .setAuthorizer(UserRoleAuthorizer)
          .buildAuthFilter()
      )
    )

    // Allow @Auth annotation injection for secured endpoints
    environment.jersey.register(new AuthValueFactoryProvider.Binder(classOf[PrincipalImpl]))
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
