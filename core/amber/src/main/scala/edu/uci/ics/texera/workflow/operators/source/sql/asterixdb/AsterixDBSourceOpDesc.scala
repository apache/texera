package edu.uci.ics.texera.workflow.operators.source.sql.asterixdb

import com.fasterxml.jackson.annotation.{
  JsonIgnoreProperties,
  JsonProperty,
  JsonPropertyDescription
}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.UIWidget
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  Schema,
  OperatorSchemaInfo
}
import edu.uci.ics.texera.workflow.operators.source.sql.{SQLSourceOpDesc, SQLSourceOpExecConfig}
import edu.uci.ics.texera.workflow.operators.source.sql.asterixdb.AsterixDBConnUtil.{
  fetchDataTypeFields,
  queryAsterixDB
}
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
  override def getKeywords: Option[String] = super.getKeywords

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig =
    new SQLSourceOpExecConfig(
      this.operatorIdentifier,
      (worker: Any) =>
        new AsterixDBSourceOpExec(
          sourceSchema(),
          host,
          port,
          database,
          table,
          limit,
          offset,
          search,
          searchByColumn,
          keywords,
          progressive,
          batchByColumn,
          min,
          max,
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

    // query dataset's Datatype from Metadata.`Datatype`
    val datasetDataType = queryAsterixDB(
      host,
      port,
      "SELECT DatatypeName FROM Metadata.`Dataset` ds where ds.`DatasetName`='" + table + "';",
      format = "JSON"
    ).get.next().asInstanceOf[JSONObject].getString("DatatypeName")

    // query field types from Metadata.`Datatype`
    val fields = fetchDataTypeFields(datasetDataType, "", host, port)

    for (key <- fields.keys.toList.sorted) {
      sb.add(new Attribute(key, attributeTypeFromAsterixDBType(fields(key))))
    }
    sb.build()
  }

  private def attributeTypeFromAsterixDBType(inputType: String): AttributeType =
    inputType match {
      case "boolean"           => AttributeType.BOOLEAN
      case "int32"             => AttributeType.INTEGER
      case "int64"             => AttributeType.LONG
      case "float" | "double"  => AttributeType.DOUBLE
      case "datetime" | "date" => AttributeType.TIMESTAMP
      case "string" | _        => AttributeType.STRING
    }
}
