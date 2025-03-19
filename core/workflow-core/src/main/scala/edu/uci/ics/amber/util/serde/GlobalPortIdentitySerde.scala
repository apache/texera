package edu.uci.ics.amber.util.serde

import org.apache.commons.codec.binary.Base32
import edu.uci.ics.amber.core.workflow.GlobalPortIdentity

/**
  * Serialize and deserializes a GlobalPortIdentity object to a string using Base32 encoding
  * to ensure it works with both URI and file path and does not incldue underscore "_" so
  * that it does not interfere with our own VFS URI parsing.
  */
object GlobalPortIdentitySerde {
  implicit class SerdeOps(globalPortId: GlobalPortIdentity) {
    def serializeAsString: String = {
      val base32 = new Base32()
      base32.encodeToString(globalPortId.toByteArray)
    }
  }

  def deserializeFromString(serializedString: String): GlobalPortIdentity = {
    val base32 = new Base32()
    val bytes = base32.decode(serializedString)
    GlobalPortIdentity.parseFrom(bytes)
  }
}
