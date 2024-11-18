package edu.uci.ics.amber.util.serde

import com.fasterxml.jackson.databind.{DeserializationContext, KeyDeserializer}
import edu.uci.ics.amber.workflow.PortIdentity

class PortIdentityKeyDeserializer extends KeyDeserializer {
  override def deserializeKey(key: String, ctxt: DeserializationContext): PortIdentity = {
    // Deserialize the string back to PortIdentity
    val parts = key.split("_")
    PortIdentity(parts(0).toInt, parts(1).toBoolean)
  }
}
