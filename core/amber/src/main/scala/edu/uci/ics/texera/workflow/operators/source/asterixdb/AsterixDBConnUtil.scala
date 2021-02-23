package edu.uci.ics.texera.workflow.operators.source.asterixdb
import kong.unirest.{HttpResponse, JsonNode, Unirest}

import scala.jdk.CollectionConverters.asScalaIteratorConverter

object AsterixDBConnUtil {

  def queryAsterixDB(
      host: String,
      port: String,
      statement: String,
      format: String = "csv"
  ): Option[Iterator[AnyRef]] = {
    val asterixAPIEndpoint = "http://" + host + ":" + port + "/query/service"

    var response: HttpResponse[JsonNode] = null

    try response = Unirest.get("http://" + host + ":" + port + "/admin/version").asJson()
    val asterixDBVersion =
      if (response.getStatus == 200) response.getBody.getObject.getString("git.build.version")

    println(asterixDBVersion)
    println(asterixAPIEndpoint)
    println(statement)
    println(if (asterixDBVersion.equals("0.9.5")) format else ("text/" + format))
    try response = Unirest
      .post(asterixAPIEndpoint)
      .header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
      .header("Accept-Language", "en-us")
      .header("Accept-Encoding", "gzip, deflate")
      .field("statement", statement)
      .field("format", if (asterixDBVersion.equals("0.9.5")) format else ("text/" + format))
//      .field("output-format", format)
      .asJson()
    catch {
      case e: Exception => e.printStackTrace()
    }
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

}
