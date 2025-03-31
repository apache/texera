package edu.uci.ics.texera.auth

import com.typesafe.config.{Config, ConfigFactory}

object AuthConfig {

  // Load configuration
  private val conf: Config = ConfigFactory.parseResources("auth.conf").resolve()

  // Read JWT expiration time
  val jwtExpirationDays: Int = conf.getInt("auth.jwt.exp-in-days")

  // Read JWT secret key
  val jwtSecretKey: String = conf.getString("auth.jwt.256-bit-secret")
}
