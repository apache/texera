package edu.uci.ics.texera.workflow.operators.source.asterixdb
import com.fasterxml.jackson.databind.JsonNode
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import scalaj.http.Http

import java.util

object AsterixDBConnUtil {
  def queryAsterixDB(
      host: String,
      port: String,
      statement: String,
      format: String = "csv"
  ): Option[util.Iterator[JsonNode]] = {
    val asterixAPIEndpoint = "http://" + host + ":" + port + "/query/service"

    val response = Http(asterixAPIEndpoint)
      .postForm(Seq("statement" -> statement, "format" -> format))
      .headers(Seq("Content-Type" -> "application/x-www-form-urlencoded", "Charset" -> "UTF-8"))
      .asString

    // parse result json from Asterixdb
    val jsonObject = objectMapper.readTree(response.body)

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
    Option(jsonObject.get("results").elements())
  }
}
