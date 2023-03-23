package edu.uci.ics.texera.workflow.operators.source.fetcher

import java.io.InputStream
import java.net.URL

object URLFetchUtil {
  def getInputStreamFromURL(urlObj: URL, retries: Int = 5): InputStream = {
    for (_ <- 0 until retries) {
      val result =
        try {
          val request = urlObj.openConnection()
          request.setRequestProperty("User-Agent", RandomUserAgent.getRandomUserAgent)
          request.getInputStream
        } catch {
          case t: Throwable => //re-try
            null
        }
      if (result != null) {
        return result
      }
    }
    throw new RuntimeException(s"Fetch Failed for URL: $urlObj")
  }
}
