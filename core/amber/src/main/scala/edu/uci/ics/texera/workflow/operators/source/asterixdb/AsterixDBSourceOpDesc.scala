package edu.uci.ics.texera.workflow.operators.source.asterixdb

import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.source.{SQLSourceOpDesc, SQLSourceOpExecConfig}

import java.sql.{Connection, SQLException}
import java.util.Collections.singletonList
import scala.jdk.CollectionConverters.asScalaBuffer

class AsterixDBSourceOpDesc extends SQLSourceOpDesc {

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
          username,
          password,
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
    updatePort()

    Schema.newBuilder().add(new Attribute("test", AttributeType.STRING)).build()

  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "AsterixDB Source",
      "Read data from a AsterixDB instance",
      OperatorGroupConstants.SOURCE_GROUP,
      List.empty,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )

  @throws[SQLException]
  override def establishConn: Connection = ???

  override def updatePort(): Unit = port = if (port.trim().equals("default")) "19002" else port

}
