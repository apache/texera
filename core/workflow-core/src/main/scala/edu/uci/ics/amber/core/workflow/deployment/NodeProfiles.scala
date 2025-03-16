package edu.uci.ics.amber.core.workflow.deployment

import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._
import java.io.File

object NodeProfiles {
  case class Profile(nodeAddress: String, modelSize: Int, modelSpeed: Float)

  def getAllProfiles: Map[String, Profile] = {
    val file = new File("profile.conf")

    // Try loading from file system first
    val config: Config =
      if (file.exists()) ConfigFactory.parseFile(file)
      else ConfigFactory.load("profile.conf") // Try loading from resources

    if (!config.hasPath("profiles")) {
      throw new RuntimeException("Missing key 'profiles' in profile.conf")
    }

    // Read profiles as a list
    val profilesList = config.getConfigList("profiles").asScala.map { profileConfig =>
      val id = profileConfig.getString("id")
      val address = profileConfig.getString("address")
      val modelSize = profileConfig.getInt("modelSize")
      val modelSpeed = profileConfig.getDouble("modelSpeed").toFloat
      address -> Profile(address, modelSize, modelSpeed)
    }
    // Convert list to a map
    profilesList.toMap
  }
}
