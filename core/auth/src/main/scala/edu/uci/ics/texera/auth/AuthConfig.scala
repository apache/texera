package edu.uci.ics.texera.auth

import com.typesafe.config.{Config, ConfigFactory}
import java.util.Random

object AuthConfig {
  // Load configuration
  private val conf: Config = ConfigFactory.parseResources("auth.conf").resolve()

  // Read JWT expiration time
  val jwtExpirationDays: Int = conf.getInt("auth.jwt.exp-in-days")

  // For storing the generated/configured secret
  @volatile private var secretKey: String = _

  // Read JWT secret key with support for random generation
  def jwtSecretKey: String = {
    synchronized {
      if (secretKey == null) {
        secretKey = conf.getString("auth.jwt.256-bit-secret").toLowerCase() match {
          case "random" => getRandomHexString
          case key      => key
        }
      }
    }
    secretKey
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
