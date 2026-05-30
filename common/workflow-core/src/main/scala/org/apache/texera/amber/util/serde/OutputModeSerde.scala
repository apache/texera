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
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode

/**
  * Serializes the scalapb enum `OutputPort.OutputMode` as its integer wire value.
  *
  * `OutputMode` is a scalapb `GeneratedEnum` (a sealed abstract class whose values are
  * `case object`s), so Jackson's default bean serialization emits an object that cannot be
  * reconstructed (the abstract class has no usable constructor). Emitting the canonical
  * protobuf integer value keeps the representation compact and unambiguous, and matches
  * how scalapb itself identifies enum values.
  */
class OutputModeSerializer extends JsonSerializer[OutputMode] {
  override def serialize(
      value: OutputMode,
      gen: JsonGenerator,
      serializers: SerializerProvider
  ): Unit = {
    gen.writeNumber(value.value)
  }
}

/**
  * Reconstructs an `OutputPort.OutputMode` from its integer wire value via
  * `OutputMode.fromValue`. For robustness it also accepts the legacy object form
  * `{"value": <int>, ...}` produced by Jackson's default bean serializer.
  */
class OutputModeDeserializer extends JsonDeserializer[OutputMode] {
  override def deserialize(
      p: JsonParser,
      ctxt: DeserializationContext
  ): OutputMode = {
    val node: JsonNode = p.getCodec.readTree(p)
    val intValue =
      if (node.isNumber) node.asInt()
      else if (node.isObject && node.has("value")) node.get("value").asInt()
      else
        throw new IllegalArgumentException(
          s"Cannot deserialize OutputPort.OutputMode from $node"
        )
    OutputMode.fromValue(intValue)
  }
}
