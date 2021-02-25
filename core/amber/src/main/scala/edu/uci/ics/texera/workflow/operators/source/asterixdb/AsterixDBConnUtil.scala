package edu.uci.ics.texera.workflow.operators.source.asterixdb
import kong.unirest.{HttpResponse, JsonNode, Unirest}

import scala.jdk.CollectionConverters.asScalaIteratorConverter

object AsterixDBConnUtil {

  // as asterixDB version update is unlikely to happen, this map
  // is only updated when a new AsterixDBSourceOpExec is initialized
  var asterixDBVersionMapping: Map[String, String] = Map()

  def queryAsterixDB(
      host: String,
      port: String,
      statement: String,
      format: String = "csv"
  ): Option[Iterator[AnyRef]] = {

    if (!asterixDBVersionMapping.contains(host)) updateAsterixDBVersionMapping(host, port)

    val asterixAPIEndpoint = "http://" + host + ":" + port + "/query/service"
    val response: HttpResponse[JsonNode] = Unirest
      .post(asterixAPIEndpoint)
      .header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
      .header("Accept-Language", "en-us")
      .header("Accept-Encoding", "gzip, deflate")
      .field("statement", statement)
      .field(
        "format",
        if (asterixDBVersionMapping(host).equals("0.9.5")) format else "text/" + format
      )
      .asJson()

    // if status is 200 OK, store the results
    if (response.getStatus == 200)
      // return results
      Option(response.getBody.getObject.getJSONArray("results").iterator().asScala)
    else
      throw new RuntimeException(
        "Send query to asterix failed: " + "error status: " + response.getStatusText + ", " +
          "error body: " + response.getBody.toString
      )

  }

  def updateAsterixDBVersionMapping(host: String, port: String): Unit = {

    var response: HttpResponse[JsonNode] = null
    // check and determine API version
    response = Unirest.get("http://" + host + ":" + port + "/admin/version").asJson()
    if (response.getStatus == 200)
      asterixDBVersionMapping += (host -> response.getBody.getObject.getString(
        "git.build.version"
      ))
  }

}
