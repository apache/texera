package edu.uci.ics.texera.workflow.operators.download

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

import java.net.URL

class URLFetcherOpExec(
    val url:String,
    val operatorSchemaInfo: OperatorSchemaInfo
                        ) extends SourceOperatorExecutor {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def produceTexeraTuple(): Iterator[Tuple] = {
    val builder = Tuple.newBuilder(operatorSchemaInfo.outputSchemas(0))
    val input = new URL(url).openStream()
    builder.addSequentially(Array(input.readAllBytes()))
    Iterator(builder.build())
  }
}
