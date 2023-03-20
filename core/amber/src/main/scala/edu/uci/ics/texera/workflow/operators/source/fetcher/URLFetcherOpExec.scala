package edu.uci.ics.texera.workflow.operators.source.fetcher

import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.operators.source.fetcher.URLFetcherOpExec.getInputStreamFromURL
import org.apache.commons.io.IOUtils

import java.io.InputStream
import java.net.URL

object URLFetcherOpExec {

  val defaultUserAgent =
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.4; en-US; rv:1.9.2.2) Gecko/20100316 Firefox/3.6.2"

  def getInputStreamFromURL(urlObj: URL): InputStream = {
    val request = urlObj.openConnection()
    request.setRequestProperty("User-Agent", defaultUserAgent)
    request.getInputStream
  }
}

class URLFetcherOpExec(
    val url: String,
    val decodingMethod: DecodingMethod,
    val operatorSchemaInfo: OperatorSchemaInfo
) extends SourceOperatorExecutor {

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def produceTexeraTuple(): Iterator[Tuple] = {
    val builder = Tuple.newBuilder(operatorSchemaInfo.outputSchemas(0))
    try {
      val urlObj = new URL(url)
      val input = getInputStreamFromURL(urlObj)
      if (decodingMethod == DecodingMethod.UTF_8) {
        builder.addSequentially(Array(IOUtils.toString(input, "UTF-8")))
      } else {
        builder.addSequentially(Array(IOUtils.toByteArray(input)))
      }
    } catch {
      case e: Throwable =>
        builder.addSequentially(Array(e.getMessage))
    }
    Iterator(builder.build())
  }
}
