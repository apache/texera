package edu.uci.ics.amber.core.storage

import io.lakefs.clients.sdk.{ApiClient, ServerConfiguration, ServerVariable}

import java.util
import scala.jdk.CollectionConverters._

/**
  * LakeFSApiClientInstance is a singleton that manages the LakeFS ApiClient instance.
  * - Provides a single shared ApiClient for all LakeFS operations in the Texera application.
  * - Lazily initializes the client on first access.
  * - Supports replacing the client instance primarily for testing or reconfiguration.
  */
object LakeFSApiClientInstance {

  private var instance: Option[ApiClient] = None

  // Constant server configuration list
  private val servers: List[ServerConfiguration] = List(
    new ServerConfiguration(
      "http://127.0.0.1:8000/api/v1",
      "lakeFS API server endpoint",
      new util.HashMap[String, ServerVariable]()
    )
  )

  private val username: String = "AKIAIOSFOLQUICKSTART"
  private val password: String = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

  /**
    * Retrieves the singleton LakeFS ApiClient instance.
    * - If the client is not initialized, it is lazily created using the configured properties.
    * @return the ApiClient instance.
    */
  def getInstance(): ApiClient = {
    instance match {
      case Some(client) => client
      case None =>
        val apiClient = new ApiClient()
        apiClient.setUsername(username)
        apiClient.setPassword(password)
        apiClient.setServers(servers.asJava)
        instance = Some(apiClient)
        apiClient
    }
  }

  /**
    * Replaces the existing LakeFS ApiClient instance.
    * - This method is useful for testing or dynamically updating the client.
    *
    * @param apiClient the new ApiClient instance to replace the current one.
    */
  def replaceInstance(apiClient: ApiClient): Unit = {
    instance = Some(apiClient)
  }
}
