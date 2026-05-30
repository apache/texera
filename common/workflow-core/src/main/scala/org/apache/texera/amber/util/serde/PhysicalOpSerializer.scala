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

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import org.apache.texera.amber.core.workflow.{PartitionInfo, PhysicalOp, PortIdentity}

/**
  * Custom Jackson serializer for [[PhysicalOp]], paired with [[PhysicalOpDeserializer]].
  *
  * It is needed for two reasons:
  *
  *  1. The runtime `inputPorts` / `outputPorts` maps hold per-port link lists plus an
  *     `Either[Throwable, Schema]`, which are not serializable. They are emitted here as
  *     the slimmed-down `inputPortsSerialized` / `outputPortsSerialized` views (links
  *     dropped, `Either` collapsed to `Option[Schema]`); the link lists are rebuilt at the
  *     `PhysicalPlan` level by replaying `links`.
  *
  *  2. `partitionRequirement` is a `List[Option[PartitionInfo]]`. `PartitionInfo` is
  *     polymorphic (`@JsonTypeInfo`), but wrapping it in `Option` / `List` erases its
  *     static type and Jackson then drops the `type` discriminator. Here each element is
  *     written through the polymorphic base-type serializer so the discriminator survives
  *     and the value can be read back.
  *
  * All other fields are written by delegating to the surrounding provider, so the
  * registered (de)serializers for `OpExecInitInfo`, `LocationPreference`,
  * `DerivePartitionSpec`, `OutputPort.OutputMode`, and the `PortIdentity` map keys are all
  * reused. Functions (`derivePartition`, `propagateSchema`) are intentionally not written.
  */
class PhysicalOpSerializer extends JsonSerializer[PhysicalOp] {

  override def serialize(
      op: PhysicalOp,
      gen: JsonGenerator,
      provider: SerializerProvider
  ): Unit = {
    gen.writeStartObject()

    gen.writeObjectField("id", op.id)
    gen.writeObjectField("workflowId", op.workflowId)
    gen.writeObjectField("executionId", op.executionId)
    gen.writeObjectField("opExecInitInfo", op.opExecInitInfo)
    gen.writeBooleanField("parallelizable", op.parallelizable)

    op.locationPreference.foreach { pref =>
      gen.writeObjectField("locationPreference", pref)
    }

    // partitionRequirement: write each element so the @JsonTypeInfo discriminator of the
    // polymorphic PartitionInfo is emitted even though the Option/List wrapper has erased
    // the static element type. We combine the concrete value serializer (which writes the
    // subtype's fields) with the base-type TypeSerializer (which writes the `type` id).
    gen.writeArrayFieldStart("partitionRequirement")
    val partitionInfoType = provider.constructType(classOf[PartitionInfo])
    val partitionTypeSerializer = provider.findTypeSerializer(partitionInfoType)
    op.partitionRequirement.foreach {
      case Some(partitionInfo) =>
        val concreteSerializer = provider.findValueSerializer(partitionInfo.getClass)
        concreteSerializer.serializeWithType(partitionInfo, gen, provider, partitionTypeSerializer)
      case None => gen.writeNull()
    }
    gen.writeEndArray()

    gen.writeObjectField("partitionDeriveSpec", op.partitionDeriveSpec)

    writePortMap(gen, provider, "inputPortsSerialized", op.inputPortsSerialized)
    writePortMap(gen, provider, "outputPortsSerialized", op.outputPortsSerialized)

    gen.writeBooleanField("isOneToManyOp", op.isOneToManyOp)
    op.suggestedWorkerNum.foreach(n => gen.writeNumberField("suggestedWorkerNum", n))
    gen.writeStringField("pveName", op.pveName)

    gen.writeEndObject()
  }

  /**
    * Writes a `Map[PortIdentity, (Port, Option[Schema])]` as a JSON object keyed by the
    * `PortIdentityKeySerializer` string key, with each value a `[port, schemaOrNull]`
    * array. This mirrors what `PhysicalOpDeserializer` reads back.
    */
  private def writePortMap[P](
      gen: JsonGenerator,
      provider: SerializerProvider,
      fieldName: String,
      portMap: Map[PortIdentity, (P, Option[org.apache.texera.amber.core.tuple.Schema])]
  ): Unit = {
    gen.writeObjectFieldStart(fieldName)
    portMap.foreach {
      case (portId, (port, schemaOpt)) =>
        gen.writeFieldName(PortIdentityKeySerializer.portIdToString(portId))
        gen.writeStartArray()
        gen.writeObject(port)
        schemaOpt match {
          case Some(schema) => gen.writeObject(schema)
          case None         => gen.writeNull()
        }
        gen.writeEndArray()
    }
    gen.writeEndObject()
  }
}
