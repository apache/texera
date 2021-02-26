package edu.uci.ics.texera.workflow.operators.source.asterixdb

import com.fasterxml.jackson.annotation.{
  JsonIgnoreProperties,
  JsonProperty,
  JsonPropertyDescription
}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.UIWidget
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.source.{SQLSourceOpDesc, SQLSourceOpExecConfig}
import edu.uci.ics.texera.workflow.operators.source.asterixdb.AsterixDBConnUtil.queryAsterixDB
import kong.unirest.json.JSONObject

import java.util.Collections.singletonList
import scala.jdk.CollectionConverters.asScalaBuffer

@JsonIgnoreProperties(value = Array("username", "password"))
class AsterixDBSourceOpDesc extends SQLSourceOpDesc {

  @JsonProperty()
  @JsonSchemaTitle("Keywords to Search")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  @JsonPropertyDescription(
    "\"['hello', 'world'], {'mode':'any'}\" OR \"['hello', 'world'], {'mode':'all'}\""
  )
  override val keywords: Option[String] = None

  override def operatorExecutor =
    new SQLSourceOpExecConfig(
      this.operatorIdentifier,
      (worker: Any) =>
        new AsterixDBSourceOpExec(
          this.sourceSchema(),
          host,
          port,
          database,
          table,
          limit,
          offset,
          column,
          keywords,
          progressive,
          batchByColumn,
          interval
        )
    )

  override def sourceSchema(): Schema = {
    if (this.host == null || this.port == null || this.database == null || this.table == null)
      return null

    querySchema
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "AsterixDB Source",
      "Read data from a AsterixDB instance",
      OperatorGroupConstants.SOURCE_GROUP,
      List.empty,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )

  override def updatePort(): Unit = port = if (port.trim().equals("default")) "19002" else port

  override def querySchema: Schema = {
    updatePort()

    val sb: Schema.Builder = Schema.newBuilder()
    if (database.equals("twitter") && table.equals("ds_tweet")) {

      // hard code for twitter.ds_tweet
      sb.add(
        new Attribute("id", AttributeType.LONG),
        new Attribute("create_at", AttributeType.TIMESTAMP),
        new Attribute("text", AttributeType.STRING),
        new Attribute("in_reply_to_status", AttributeType.LONG),
        new Attribute("in_reply_to_user", AttributeType.LONG),
        new Attribute("favorite_count", AttributeType.LONG),
        new Attribute("retweet_count", AttributeType.LONG),
        new Attribute("lang", AttributeType.STRING),
        new Attribute("is_retweet", AttributeType.BOOLEAN),
        new Attribute("hashtags", AttributeType.STRING),
        new Attribute("user_mentions", AttributeType.STRING),
        new Attribute("user_id", AttributeType.LONG),
        new Attribute("user_name", AttributeType.STRING),
        new Attribute("user_screen_name", AttributeType.STRING),
        new Attribute("user_location", AttributeType.STRING),
        new Attribute("user_description", AttributeType.STRING),
        new Attribute("user_followers_count", AttributeType.LONG),
        new Attribute("user_friends_count", AttributeType.LONG),
        new Attribute("user_statues_count", AttributeType.LONG),
        new Attribute("stateName", AttributeType.STRING),
        new Attribute("countyName", AttributeType.STRING),
        new Attribute("cityName", AttributeType.STRING),
        new Attribute("country", AttributeType.STRING),
        new Attribute("bounding_box", AttributeType.STRING)
      )

    } else {

      // query and match types from Metadata.`Datatype`
      val ASTERIXDB_GET_SCHEMA_QUERY: String =
        "SELECT dt.Derived.Record.Fields FROM Metadata.`Datatype` dt, (SELECT DatatypeName FROM Metadata.`Dataset` ds " +
          "where ds.`DatasetName`=\"" + table + "\") as dn where dt.DatatypeName =dn.DatatypeName;"
      val fields = queryAsterixDB(host, port, ASTERIXDB_GET_SCHEMA_QUERY, format = "JSON")
      fields.get
        .next()
        .asInstanceOf[JSONObject]
        .getJSONArray("Fields")
        .forEach(field => {
          val fieldName: String = field.asInstanceOf[JSONObject].get("FieldName").toString
          val fieldType: String = field.asInstanceOf[JSONObject].get("FieldType").toString
          sb.add(new Attribute(fieldName, attributeTypeFromAsterixDBType(fieldType)))
        })
    }
    sb.build()
  }

  private def attributeTypeFromAsterixDBType(inputType: String): AttributeType =
    inputType match {
      case "boolean"          => AttributeType.BOOLEAN
      case "int32"            => AttributeType.INTEGER
      case "int64"            => AttributeType.LONG
      case "float" | "double" => AttributeType.DOUBLE
      case "datetime"         => AttributeType.STRING
      case "string" | _       => AttributeType.STRING
    }
}
