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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.{
  DeserializationContext,
  JsonDeserializer,
  JsonNode,
  ObjectMapper
}
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{
  DerivePartitionSpec,
  InputPort,
  LocationPreference,
  OutputPort,
  PartitionInfo,
  Passthrough,
  PhysicalOp,
  PortIdentity
}

/**
  * Custom Jackson deserializer for [[PhysicalOp]].
  *
  * jackson-module-scala binds Scala case classes to their primary constructor, whose
  * `inputPorts` / `outputPorts` maps (`Either[Throwable, Schema]` + lazy vals) and
  * `propagateSchema` / `derivePartition` function values cannot be parsed from JSON.
  * Serialization instead emits the slimmed-down `inputPortsSerialized` /
  * `outputPortsSerialized` views and the `partitionDeriveSpec`; this deserializer reads
  * those back and delegates to [[PhysicalOp.fromSerialized]] to rebuild the real maps
  * (with empty link lists, to be rehydrated at the `PhysicalPlan` level) and the lazy
  * `derivePartition` function.
  *
  * Each nested field is decoded through the surrounding mapper, so all other registered
  * (de)serializers (e.g. for `OpExecInitInfo`, `LocationPreference`, `PartitionInfo`,
  * `DerivePartitionSpec`, and the `PortIdentity` map keys) are reused.
  */
class PhysicalOpDeserializer extends JsonDeserializer[PhysicalOp] {

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): PhysicalOp = {
    val mapper = p.getCodec.asInstanceOf[ObjectMapper]
    val node: JsonNode = mapper.readTree(p)

    def required[T](field: String, clazz: Class[T]): T = {
      val child = node.get(field)
      if (child == null || child.isNull) {
        throw new IllegalArgumentException(
          s"Cannot deserialize PhysicalOp: missing required field '$field' in $node"
        )
      }
      mapper.treeToValue(child, clazz)
    }

    def optionalNode(field: String): Option[JsonNode] = {
      val child = node.get(field)
      if (child == null || child.isNull) None else Some(child)
    }

    val id = required("id", classOf[PhysicalOpIdentity])
    val workflowId = required("workflowId", classOf[WorkflowIdentity])
    val executionId = required("executionId", classOf[ExecutionIdentity])
    val opExecInitInfo = required("opExecInitInfo", classOf[OpExecInitInfo])

    val parallelizable =
      optionalNode("parallelizable").forall(_.asBoolean(true))
    val isOneToManyOp =
      optionalNode("isOneToManyOp").exists(_.asBoolean(false))
    val pveName =
      optionalNode("pveName").map(_.asText("")).getOrElse("")

    val locationPreference: Option[LocationPreference] =
      optionalNode("locationPreference").map(n => mapper.treeToValue(n, classOf[LocationPreference]))

    val suggestedWorkerNum: Option[Int] =
      optionalNode("suggestedWorkerNum").map(_.asInt())

    val partitionDeriveSpec: DerivePartitionSpec =
      optionalNode("partitionDeriveSpec")
        .map(n => mapper.treeToValue(n, classOf[DerivePartitionSpec]))
        .getOrElse(Passthrough())

    // List[Option[PartitionInfo]] — decode element-wise to preserve nulls as None.
    val partitionRequirement: List[Option[PartitionInfo]] =
      optionalNode("partitionRequirement") match {
        case Some(arr) if arr.isArray =>
          val builder = List.newBuilder[Option[PartitionInfo]]
          arr.forEach { elem =>
            if (elem == null || elem.isNull) builder += None
            else builder += Some(mapper.treeToValue(elem, classOf[PartitionInfo]))
          }
          builder.result()
        case _ => List.empty
      }

    val inputPortsSerialized =
      optionalNode("inputPortsSerialized")
        .map(decodeInputPorts(mapper, _))
        .getOrElse(Map.empty[PortIdentity, (InputPort, Option[Schema])])
    val outputPortsSerialized =
      optionalNode("outputPortsSerialized")
        .map(decodeOutputPorts(mapper, _))
        .getOrElse(Map.empty[PortIdentity, (OutputPort, Option[Schema])])

    PhysicalOp.fromSerialized(
      id = id,
      workflowId = workflowId,
      executionId = executionId,
      opExecInitInfo = opExecInitInfo,
      parallelizable = parallelizable,
      locationPreference = locationPreference,
      partitionRequirement = partitionRequirement,
      partitionDeriveSpec = partitionDeriveSpec,
      inputPortsSerialized = inputPortsSerialized,
      outputPortsSerialized = outputPortsSerialized,
      isOneToManyOp = isOneToManyOp,
      suggestedWorkerNum = suggestedWorkerNum,
      pveName = pveName
    )
  }

  /**
    * Decodes the `{ "<portKey>": [port, schemaOrNull], ... }` object emitted for the
    * serialized input-port view. The keys are decoded via the registered
    * `PortIdentityKeyDeserializer` (mirroring `PortIdentityKeySerializer`).
    */
  private def decodeInputPorts(
      mapper: ObjectMapper,
      node: JsonNode
  ): Map[PortIdentity, (InputPort, Option[Schema])] =
    decodePortMap(mapper, node, classOf[InputPort])

  private def decodeOutputPorts(
      mapper: ObjectMapper,
      node: JsonNode
  ): Map[PortIdentity, (OutputPort, Option[Schema])] =
    decodePortMap(mapper, node, classOf[OutputPort])

  private def decodePortMap[P](
      mapper: ObjectMapper,
      node: JsonNode,
      portClass: Class[P]
  ): Map[PortIdentity, (P, Option[Schema])] = {
    if (!node.isObject) {
      return Map.empty[PortIdentity, (P, Option[Schema])]
    }
    val builder = Map.newBuilder[PortIdentity, (P, Option[Schema])]
    val fields = node.fields()
    while (fields.hasNext) {
      val entry = fields.next()
      val portId = parsePortKey(entry.getKey)
      val tupleNode = entry.getValue
      // The Scala module serializes a Tuple2 as a 2-element JSON array: [port, schema?].
      val portNode = tupleNode.get(0)
      val schemaNode = tupleNode.get(1)
      val port = mapper.treeToValue(portNode, portClass)
      val schemaOpt: Option[Schema] =
        if (schemaNode == null || schemaNode.isNull) None
        else Some(mapper.treeToValue(schemaNode, classOf[Schema]))
      builder += portId -> ((port, schemaOpt))
    }
    builder.result()
  }

  /**
    * Parses the `"<id>_<internal>"` port key produced by `PortIdentityKeySerializer`.
    */
  private def parsePortKey(key: String): PortIdentity = {
    val parts = key.split("_")
    PortIdentity(parts(0).toInt, parts(1).toBoolean)
  }
}
