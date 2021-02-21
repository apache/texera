package edu.uci.ics.texera.workflow.operators.source.asterixdb
import com.fasterxml.jackson.core.json.JsonReadFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.json.JsonMapper
import scalaj.http.{Http, HttpResponse}

import scala.jdk.CollectionConverters.asScalaIteratorConverter

object AsterixDBConnUtil {

  // cannot use the Util.objectMapper, some special configurations needs to be performed.
  val jsonMapper: JsonMapper = JsonMapper
    .builder()
    .enable(
      JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(),
      JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER.mappedFeature()
    )
    .build()

  def queryAsterixDB(
      host: String,
      port: String,
      statement: String,
      format: String = "csv"
  ): Option[Iterator[JsonNode]] = {
    val asterixAPIEndpoint = "http://" + host + ":" + port + "/query/service"

    val response: HttpResponse[String] = Http(asterixAPIEndpoint)
      .timeout(connTimeoutMs = 10000, readTimeoutMs = 10000)
      .postForm(Seq("statement" -> statement, "format" -> format))
      .headers(
        Seq(
          "Content-Type" -> "application/x-www-form-urlencoded; charset=UTF-8",
          "Accept-Language" -> "en-us",
          "Accept-Encoding" -> "gzip, deflate"
        )
      )
      .asString // FIXME: this would result in some encoding errors

    println("json result ", response)
    // parse result json from Asterixdb

    val jsonObject = jsonMapper.readTree(response.body)

    if (!jsonObject.get("status").textValue.equals("success")) {
      // report error from AsterixDB
      val sb: StringBuilder = new StringBuilder()
      jsonObject
        .get("errors")
        .elements()
        .forEachRemaining((error: JsonNode) => {
          sb.append(error.get("code").intValue() + error.get("msg").textValue() + "\n")
        })

      throw new RuntimeException(
        s"Execution errors against $host:$port {\n" + sb.result() + "}\n"
      )
    }

    // return results
    Option(jsonObject.get("results").elements().asScala)
  }
}
