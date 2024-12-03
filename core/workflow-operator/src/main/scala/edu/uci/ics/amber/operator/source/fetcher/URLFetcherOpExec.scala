package edu.uci.ics.amber.operator.source.fetcher

import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.tuple.TupleLike
import edu.uci.ics.amber.operator.source.fetcher.URLFetchUtil.getInputStreamFromURL
import org.apache.commons.io.IOUtils

import java.net.URL

class URLFetcherOpExec(url: String, decodingMethod: DecodingMethod) extends SourceOperatorExecutor {

  override def produceTuple(): Iterator[TupleLike] = {

    val urlObj = new URL(url)
    val input = getInputStreamFromURL(urlObj)
    val contentInputStream = input match {
      case Some(value) => value
      case None        => IOUtils.toInputStream(s"Fetch failed for URL: $url", "UTF-8")
    }
    Iterator(if (decodingMethod == DecodingMethod.UTF_8) {
      TupleLike(IOUtils.toString(contentInputStream, "UTF-8"))
    } else {
      TupleLike(IOUtils.toByteArray(contentInputStream))
    })
  }
}
