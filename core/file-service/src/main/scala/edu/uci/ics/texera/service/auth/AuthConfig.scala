package edu.uci.ics.texera.service.auth

import org.yaml.snakeyaml.Yaml

import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._

object AuthConfig {
  private val conf: Map[String, Any] = {
    val yaml = new Yaml()
    val inputStream = getClass.getClassLoader.getResourceAsStream("auth-config.yaml")
    val javaConf = yaml.load(inputStream).asInstanceOf[JMap[String, Any]].asScala.toMap

    val authMap = javaConf("auth").asInstanceOf[JMap[String, Any]].asScala.toMap
    val jwtMap = authMap("jwt").asInstanceOf[JMap[String, Any]].asScala.toMap

    javaConf.updated(
      "auth",
      authMap.updated("jwt", jwtMap)
    )
  }

  // Read JWT expiration time
  val jwtExpirationDays: Int = conf("auth")
    .asInstanceOf[Map[String, Any]]("jwt")
    .asInstanceOf[Map[String, Any]]("exp-in-days")
    .asInstanceOf[Int]

  // Read JWT secret key
  val jwtSecretKey: String = conf("auth")
    .asInstanceOf[Map[String, Any]]("jwt")
    .asInstanceOf[Map[String, Any]]("256-bit-secret")
    .asInstanceOf[String]
}
