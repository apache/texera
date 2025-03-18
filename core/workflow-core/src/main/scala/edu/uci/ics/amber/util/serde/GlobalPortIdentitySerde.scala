package edu.uci.ics.amber.util.serde

import edu.uci.ics.amber.core.workflow.GlobalPortIdentity

import java.net.{URLDecoder, URLEncoder}
import java.util.Base64

object GlobalPortIdentitySerde {
  implicit class SerdeOps(msg: GlobalPortIdentity) {
    // Serialize GlobalPortIdentity to a URL-safe Base64 string
    def toUriString: String = {
      val base64 = Base64.getUrlEncoder.encodeToString(msg.toByteArray)
      URLEncoder.encode(base64, "UTF-8") // Ensure it's fully URI-safe
    }
  }

  // Deserialize GlobalPortIdentity from a URL-safe Base64 string
  def fromUriString(encodedStr: String): GlobalPortIdentity = {
    val base64 = URLDecoder.decode(encodedStr, "UTF-8")
    val bytes = Base64.getUrlDecoder.decode(base64)
    GlobalPortIdentity.parseFrom(bytes)
  }
}