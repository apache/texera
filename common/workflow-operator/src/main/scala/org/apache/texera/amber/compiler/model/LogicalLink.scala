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

package org.apache.texera.amber.compiler.model

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.PortIdentity

// Serialized by LogicalLinkSerializer, which writes fromOpId / toOpId as bare string ids — the same
// shape the @JsonCreator string constructor reads — so a LogicalLink round-trips through JSON.
@JsonSerialize(using = classOf[LogicalLinkSerializer])
case class LogicalLink(
    @JsonProperty("fromOpId") fromOpId: OperatorIdentity,
    fromPortId: PortIdentity,
    @JsonProperty("toOpId") toOpId: OperatorIdentity,
    toPortId: PortIdentity
) {
  require(
    fromOpId != null && fromOpId.id != null && fromOpId.id.nonEmpty,
    "LogicalLink fromOpId must be non-null and non-empty"
  )
  require(
    toOpId != null && toOpId.id != null && toOpId.id.nonEmpty,
    "LogicalLink toOpId must be non-null and non-empty"
  )
  require(
    fromOpId != toOpId,
    s"LogicalLink self-loop not allowed: fromOpId == toOpId == ${fromOpId.id}"
  )

  @JsonCreator
  def this(
      @JsonProperty("fromOpId") fromOpId: String,
      fromPortId: PortIdentity,
      @JsonProperty("toOpId") toOpId: String,
      toPortId: PortIdentity
  ) = {
    this(OperatorIdentity(fromOpId), fromPortId, OperatorIdentity(toOpId), toPortId)
  }
}

/**
  * Emits `fromOpId` / `toOpId` as bare string ids (not the `{"id": ...}` object form Jackson would
  * derive from the `OperatorIdentity` case class), matching the shape the `@JsonCreator` string
  * constructor consumes. Without this, `writeValueAsString` produces JSON that the link's own
  * deserializer cannot read back. Ports keep their default object serialization. See
  * https://github.com/apache/texera/issues/5042. The ComputingUnitMaster ->
  * workflow-compiling-service path relies on this round-trip (it re-serializes a logical plan
  * and ships it over HTTP).
  */
class LogicalLinkSerializer extends JsonSerializer[LogicalLink] {
  override def serialize(
      link: LogicalLink,
      gen: JsonGenerator,
      provider: SerializerProvider
  ): Unit = {
    gen.writeStartObject()
    gen.writeStringField("fromOpId", link.fromOpId.id)
    gen.writeFieldName("fromPortId")
    provider.defaultSerializeValue(link.fromPortId, gen)
    gen.writeStringField("toOpId", link.toOpId.id)
    gen.writeFieldName("toPortId")
    provider.defaultSerializeValue(link.toPortId, gen)
    gen.writeEndObject()
  }
}
