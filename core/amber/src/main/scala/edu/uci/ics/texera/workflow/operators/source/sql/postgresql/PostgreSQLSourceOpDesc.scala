package edu.uci.ics.texera.workflow.operators.source.sql.postgresql

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.UIWidget
import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.operators.source.sql.{SQLSourceOpDesc, SQLSourceOpExecConfig}
import edu.uci.ics.texera.workflow.operators.source.sql.postgresql.PostgreSQLConnUtil.connect

import java.sql.{Connection, SQLException}
import java.util.Collections.singletonList
import scala.jdk.CollectionConverters.asScalaBuffer

class PostgreSQLSourceOpDesc extends SQLSourceOpDesc {

  @JsonProperty()
  @JsonSchemaTitle("Keywords to Search")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  @JsonPropertyDescription(
    "E.g. 'sore & throat' for AND; 'sore', 'throat' for OR. See official postgres documents for details."
  )
  override def getKeywords: Option[String] = super.getKeywords

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig =
    new SQLSourceOpExecConfig(
      operatorIdentifier,
      (worker: Any) =>
        new PostgreSQLSourceOpExec(
          querySchema,
          host,
          port,
          database,
          table,
          username,
          password,
          limit,
          offset,
          progressive,
          batchByColumn,
          min,
          max,
          interval,
          keywordSearch.getOrElse(false),
          keywordSearchByColumn.orNull,
          keywords.orNull
        )
    )
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "PostgreSQL Source",
      "Read data from a PostgreSQL instance",
      OperatorGroupConstants.SOURCE_GROUP,
      List.empty,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )

  @throws[SQLException]
  override def establishConn: Connection = connect(host, port, database, username, password)

  override protected def updatePort(): Unit =
    port = if (port.trim().equals("default")) "5432" else port
}
