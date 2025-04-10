package edu.uci.ics.amber.operator.source.fetcher

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.operator.source.fetcher.URLFetchUtil.getInputStreamFromURL
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.apache.commons.io.IOUtils

import java.net.URL

class URLFetcherOpExec(descString: String) extends SourceOperatorExecutor {
  private val desc: URLFetcherOpDesc = objectMapper.readValue(descString, classOf[URLFetcherOpDesc])
  override def produceTuple(): Iterator[TupleLike] = {

    val urlObj = new URL(desc.url)
    val input = getInputStreamFromURL(urlObj)
    val contentInputStream = input match {
      case Some(value) => value
      case None        => IOUtils.toInputStream(s"Fetch failed for URL: $desc.url", "UTF-8")
    }
    Iterator(if (desc.decodingMethod == DecodingMethod.UTF_8) {
      TupleLike(IOUtils.toString(contentInputStream, "UTF-8"))
    } else {
      TupleLike(IOUtils.toByteArray(contentInputStream))
    })
  }
}
