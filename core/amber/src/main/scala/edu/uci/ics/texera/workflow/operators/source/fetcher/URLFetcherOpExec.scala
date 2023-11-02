package edu.uci.ics.texera.workflow.operators.source.fetcher

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.operators.source.fetcher.URLFetchUtil.getInputStreamFromURL
import org.apache.commons.io.IOUtils

import java.net.URL

class URLFetcherOpExec(
    val url: String,
    val decodingMethod: DecodingMethod,
    val operatorSchemaInfo: OperatorSchemaInfo
) extends SourceOperatorExecutor {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def produceTexeraTuple(): Iterator[Tuple] = {
    val builder = Tuple.newBuilder(operatorSchemaInfo.outputSchemas(0))
    val urlObj = new URL(url)
    val input = getInputStreamFromURL(urlObj)
    val contentInputStream = input match {
      case Some(value) => value
      case None        => IOUtils.toInputStream(s"Fetch failed for URL: $url", "UTF-8")
    }
    if (decodingMethod == DecodingMethod.UTF_8) {
      builder.addSequentially(Array(IOUtils.toString(contentInputStream, "UTF-8")))
    } else {
      builder.addSequentially(Array(IOUtils.toByteArray(contentInputStream)))
    }
    Iterator(builder.build())
  }
}
