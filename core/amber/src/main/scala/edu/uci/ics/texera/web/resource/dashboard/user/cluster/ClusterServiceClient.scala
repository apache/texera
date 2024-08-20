package edu.uci.ics.texera.web.resource.dashboard.user.cluster

import edu.uci.ics.amber.engine.common.AmberConfig

import java.net.{HttpURLConnection, URL}
import scala.io.Source
import scala.util.{Failure, Success, Try}

object ClusterServiceClient {

  /**
    * Makes an HTTP POST request to create a cluster in the Go microservice.
    *
    * @param clusterId        The id of the cluster.
    * @param machineType      The type of the machine for the cluster.
    * @param numberOfMachines The number of machines in the cluster.
    * @return Either an error message in Left, or the response body in Right.
    */
  def callCreateClusterAPI(
      clusterId: Int,
      machineType: String,
      numberOfMachines: Int
  ): Either[String, String] = {
    val url = new URL(s"${AmberConfig.clusterLauncherServiceTarget}/api/cluster/create")
    val jsonInputString =
      s"""{
         |"provider": "aws",
         |"machineType": "$machineType",
         |"numberOfNodes": $numberOfMachines,
         |"clusterId": $clusterId
         |}""".stripMargin

    sendHttpRequest("POST", url, Some(jsonInputString))
  }

  /**
    * Makes an HTTP DELETE request to delete a cluster in the Go microservice.
    *
    * @param clusterId The ID of the cluster to be deleted.
    * @return Either an error message in Left, or the response body in Right.
    */
  def callDeleteClusterAPI(clusterId: Int): Either[String, String] = {
    val url = new URL(s"${AmberConfig.clusterLauncherServiceTarget}/api/cluster/$clusterId")
    sendHttpRequest("DELETE", url, None)
  }

  /**
    * Helper function to send an HTTP request.
    *
    * @param method          The HTTP method (e.g., POST, DELETE).
    * @param url             The URL for the HTTP request.
    * @param jsonInputString The optional JSON payload for the request body (for POST requests).
    * @return Either an error message in Left, or the response body in Right.
    */
  private def sendHttpRequest(
      method: String,
      url: URL,
      jsonInputString: Option[String]
  ): Either[String, String] = {
    Try {
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod(method)
      conn.setRequestProperty("Content-Type", "application/json")
      conn.setDoOutput(jsonInputString.isDefined)

      jsonInputString.foreach { input =>
        val os = conn.getOutputStream
        os.write(input.getBytes("UTF-8"))
        os.close()
      }

      val responseCode = conn.getResponseCode
      val result = if (responseCode == HttpURLConnection.HTTP_OK) {
        Right(Source.fromInputStream(conn.getInputStream).mkString)
      } else {
        Left(s"Failed: HTTP error code $responseCode")
      }

      conn.disconnect()
      result
    } match {
      case Success(result) => result
      case Failure(exception) =>
        Left(s"Error: ${exception.getMessage}")
    }
  }
}
