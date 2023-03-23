package edu.uci.ics.texera.workflow.operators.source.fetcher

import java.io.InputStream
import java.net.URL

object URLFetchUtil {
  def getInputStreamFromURL(urlObj: URL, retries: Int = 5): Option[InputStream] = {
    for (_ <- 0 until retries) {
      val result =
        try {
          val request = urlObj.openConnection()
          request.setRequestProperty("User-Agent", RandomUserAgent.getRandomUserAgent)
          Some(request.getInputStream)
        } catch {
          case t: Throwable => //re-try
            None
        }
      if (result.isDefined) {
        return result
      }
    }
    None
  }
}
