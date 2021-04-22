package edu.uci.ics.texera.workflow.operators.source.sql.asterixdb
import kong.unirest.{HttpResponse, JsonNode, Unirest}
import kong.unirest.json.JSONObject

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.jdk.CollectionConverters.asScalaIteratorConverter
import scala.util.{Failure, Success, Try}
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
    if (response.getStatus == 200) {
      // return results
      Option(response.getBody.getObject.getJSONArray("results").iterator().asScala)
    } else
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

  def fetchDataTypeFields(
      datatypeName: String,
      parentName: String,
      host: String,
      port: String
  ): Predef.Map[String, String] = {
    val result: mutable.Map[String, String] = mutable.Map()
    val response = queryAsterixDB(
      host,
      port,
      s"SELECT dt.Derived.Record.Fields FROM Metadata.`Datatype` dt where dt.DatatypeName = '$datatypeName';",
      format = "JSON"
    )

    Try(
      response.get
        .next()
        .asInstanceOf[JSONObject]
        .getJSONArray("Fields")
    ) match {
      case Success(fields) =>
        fields.forEach(field => {
          val fieldName: String = field.asInstanceOf[JSONObject].get("FieldName").toString
          val fieldType: String = field.asInstanceOf[JSONObject].get("FieldType").toString
          val fieldNameWithParent: String =
            (if (parentName.nonEmpty) parentName + "." else "") + fieldName

          if (fieldType.contains("type")) {
            val childMap =
              fetchDataTypeFields(
                fieldType,
                fieldNameWithParent,
                host,
                port
              )
            result ++= childMap
          } else {
            result.put(fieldNameWithParent, fieldType)
          }
        })
      case Failure(_) =>
      // could due to the following reasons:
      //    the specific type's metadata is not found
      //    the current model does not have a good support for type of arrays, thus arrays are ignored

    }
    result.toMap
  }

}
