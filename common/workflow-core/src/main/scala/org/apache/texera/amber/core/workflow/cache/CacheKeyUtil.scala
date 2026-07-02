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

package org.apache.texera.amber.core.workflow.cache

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalLink,
  PhysicalOp,
  PhysicalPlan
}
import org.apache.texera.amber.util.JSONUtils

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/**
  * The cache key of an output port.
  *
  * @param json the JSON describing the upstream sub-DAG the key is computed from
  * @param hash SHA-256 hash of `json`; cache lookups match on this hash and then
  *             confirm the match with `json` (see [[CacheKeyUtil.isSameComputation]])
  */
case class StorageCacheKey(json: String, hash: String)

/**
  * Computes deterministic cache keys for an output port from its upstream sub-DAG.
  *
  * The sub-DAG is the port's operator together with its transitive upstream operators
  * (see [[PhysicalPlan.getTransitiveUpstreamSubPlan]]); the caller passes it in. The JSON
  * payload captures:
  *   - the target output port,
  *   - all operators in the sub-DAG (sorted),
  *   - their exec init info (proto string),
  *   - their output schemas (string form when available),
  *   - all edges between those operators (sorted).
  *
  * The payload is serialized with ordered keys and hashed with SHA-256. Identical sub-DAGs
  * produce identical hashes; any change in structure or configuration changes the hash. Sort
  * orders use the full port identity (id + internal flag) so the payload is stable regardless
  * of Map/Set iteration order.
  */
object CacheKeyUtil {

  /**
    * Compute the cache key of the given upstream `subDag` for the output port `target`.
    *
    * Contract: the caller provides `target` as an output port and `subDag` as the sub-DAG that
    * produces it, that is `plan.getTransitiveUpstreamSubPlan(target.opId)`. This method does not
    * re-validate that relationship; it hashes whatever sub-DAG it is handed. The payload uses
    * sorted keys and is hashed with SHA-256.
    */
  def computeCacheKey(
      subDag: PhysicalPlan,
      target: GlobalPortIdentity
  ): StorageCacheKey = {
    val payload = buildJSONPayload(subDag.operators, subDag.links, target)
    val json = objectMapper.writeValueAsString(payload)
    StorageCacheKey(json, sha256Hex(json))
  }

  /**
    * Whether two cache keys identify the same upstream computation.
    *
    * The hash is compared first; on a hash match the full JSON is compared as well. This
    * keeps the match safe against a hash collision: if two different computations ever
    * produced the same hash, their JSON would still differ, so they are reported as different
    * and a cached result is never reused for a port it was not computed from.
    */
  def isSameComputation(a: StorageCacheKey, b: StorageCacheKey): Boolean =
    a.hash == b.hash && a.json == b.json

  /**
    * Build the JSON payload describing the sub-DAG:
    *  - target port
    *  - sorted nodes with exec info and schemas
    *  - sorted edges
    */
  private def buildJSONPayload(
      nodes: Set[PhysicalOp],
      links: Set[PhysicalLink],
      target: GlobalPortIdentity
  ): ObjectNode = {
    val root = objectMapper.createObjectNode()
    // target.toString is used only as a stable discriminator inside the hash; it is
    // NOT the GlobalPortIdentitySerde form stored in the operator_port_cache table.
    root.put("targetPort", target.toString)

    val nodeArray = objectMapper.createArrayNode()
    nodes.toList
      .sortBy(_.id.toString)
      .foreach(op => nodeArray.add(buildNode(op)))
    root.set("nodes", nodeArray)

    val edgeArray = objectMapper.createArrayNode()
    links.toList
      .sortBy(link =>
        (
          link.fromOpId.toString,
          link.fromPortId.id,
          link.fromPortId.internal,
          link.toOpId.toString,
          link.toPortId.id,
          link.toPortId.internal
        )
      )
      .foreach(link => edgeArray.add(buildEdge(link)))
    root.set("edges", edgeArray)

    root
  }

  /**
    * Serialize a physical operator into a deterministic JSON node.
    * Captures IDs, exec init info, and output schemas.
    */
  private def buildNode(op: PhysicalOp): ObjectNode = {
    val node = objectMapper.createObjectNode()
    node.put("physicalOpId", op.id.toString)
    node.put("logicalOpId", op.id.logicalOpId.toString)
    node.set("opExec", serializeOpExec(op.opExecInitInfo))

    val schemaArray = objectMapper.createArrayNode()
    op.outputPorts.toList
      .sortBy(p => (p._1.id, p._1.internal))
      .foreach {
        case (portId, (_, _, schemaEither)) =>
          val schemaNode = objectMapper.createObjectNode()
          schemaNode.put("portId", portId.id)
          schemaNode.put("internal", portId.internal)
          schemaEither.toOption match {
            case Some(schema) =>
              schemaNode.put("available", true)
              schemaNode.put("schemaString", schema.toString)
            case None =>
              schemaNode.put("available", false)
          }
          schemaArray.add(schemaNode)
      }
    node.set("outputSchemas", schemaArray)

    node
  }

  /**
    * Serialize a physical link into a deterministic JSON node.
    */
  private def buildEdge(link: PhysicalLink): ObjectNode = {
    val edge = objectMapper.createObjectNode()
    edge.put("fromOpId", link.fromOpId.toString)
    edge.put("fromPortId", link.fromPortId.id)
    edge.put("fromInternal", link.fromPortId.internal)
    edge.put("toOpId", link.toOpId.toString)
    edge.put("toPortId", link.toPortId.id)
    edge.put("toInternal", link.toPortId.internal)
    edge
  }

  // Derived from the shared JSONUtils.objectMapper so configuration stays in sync, with
  // ORDER_MAP_ENTRIES_BY_KEYS added on top: the cache key must serialize map keys in a stable
  // order so the same sub-DAG always produces the same hash. The shared mapper does not set
  // this, so reusing it directly would risk non-deterministic keys.
  private val objectMapper =
    JSONUtils.objectMapper.copy().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)

  private def sha256Hex(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8))
    bytes.map("%02x".format(_)).mkString
  }

  /**
    * Serialize the operator's exec init info deterministically via its proto string.
    *
    * The cache key is computed at the physical-plan layer, where the operator Desc is not
    * available on a `PhysicalOp`; only `opExecInitInfo` is. It is also the concrete execution
    * definition the engine actually runs, so hashing it ties the key to exactly what produces a
    * port's result, not to a higher-level description that could map to different executions. If
    * what executes a port changes, the key changes, so a stale result is never reused.
    */
  private def serializeOpExec(opExecInitInfo: OpExecInitInfo): ObjectNode = {
    val n = objectMapper.createObjectNode()
    n.put("protoString", opExecInitInfo.asMessage.toProtoString)
    n
  }
}
