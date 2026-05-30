/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.util.serde

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{
  DeserializationContext,
  JsonDeserializer,
  JsonNode,
  JsonSerializer,
  SerializerProvider
}
import org.apache.texera.amber.core.workflow.{
  LocationPreference,
  PreferController,
  RoundRobinPreference
}

/**
  * Discriminator values used in the JSON representation of [[LocationPreference]].
  * Kept in one place so the serializer and deserializer cannot drift apart.
  */
private object LocationPreferenceSerde {
  val PreferControllerType = "preferController"
  val RoundRobinType = "roundRobin"
}

/**
  * Serializes a [[LocationPreference]] singleton as `{"type": "<discriminator>"}`.
  */
class LocationPreferenceSerializer extends JsonSerializer[LocationPreference] {
  override def serialize(
      value: LocationPreference,
      gen: JsonGenerator,
      serializers: SerializerProvider
  ): Unit = {
    val typeName = value match {
      case PreferController     => LocationPreferenceSerde.PreferControllerType
      case RoundRobinPreference => LocationPreferenceSerde.RoundRobinType
    }
    gen.writeStartObject()
    gen.writeStringField("type", typeName)
    gen.writeEndObject()
  }
}

/**
  * Deserializes a [[LocationPreference]] from `{"type": "<discriminator>"}`, always
  * returning the canonical singleton instance so that `eq` identity and pattern
  * matching keep working after a round-trip.
  */
class LocationPreferenceDeserializer extends JsonDeserializer[LocationPreference] {
  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext
  ): LocationPreference = {
    val node: JsonNode = p.getCodec.readTree(p)
    val typeNode = node.get("type")
    if (typeNode == null) {
      throw new IllegalArgumentException(
        s"Cannot deserialize LocationPreference: missing 'type' field in $node"
      )
    }
    typeNode.asText() match {
      case LocationPreferenceSerde.PreferControllerType => PreferController
      case LocationPreferenceSerde.RoundRobinType       => RoundRobinPreference
      case other =>
        throw new IllegalArgumentException(s"Unknown LocationPreference type: $other")
    }
  }
}
