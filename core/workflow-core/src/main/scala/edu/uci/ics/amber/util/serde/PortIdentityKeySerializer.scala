package edu.uci.ics.amber.util.serde

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import edu.uci.ics.amber.workflow.PortIdentity

class PortIdentityKeySerializer extends JsonSerializer[PortIdentity] {
  override def serialize(
      key: PortIdentity,
      gen: JsonGenerator,
      serializers: SerializerProvider
  ): Unit = {
    // Serialize PortIdentity as a string "id_internal"
    gen.writeFieldName(s"${key.id}_${key.internal}")
  }
}
