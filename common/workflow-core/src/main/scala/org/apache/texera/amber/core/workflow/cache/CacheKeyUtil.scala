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

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalLink,
  PhysicalOp,
  PhysicalPlan
}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import scala.collection.mutable

/**
  * The cache key of an output port.
  *
  * @param json the JSON describing the upstream sub-DAG the key is computed from
  * @param hash SHA-256 hash of `json`; cache lookups match on this hash and then
  *             confirm the match with `json` (see [[CacheKeyUtil.sameComputation]])
  */
case class CacheKey(json: String, hash: String)

/**
  * Computes deterministic cache keys from the upstream sub-DAG of an output port.
  *
  * The payload captures:
  *   - the target output port,
  *   - all upstream physical operators (sorted),
  *   - their exec init info (proto string),
  *   - their output schemas (string form when available),
  *   - all edges between those operators (sorted).
  *
  * The payload is serialized with ordered keys and hashed with SHA-256. Identical
  * input plans produce identical hashes; any change in structure or configuration
  * changes the hash. Sort orders use the full port identity (id + internal flag) so
  * the payload is stable regardless of Map/Set iteration order.
  */
object CacheKeyUtil {

  /**
    * Compute the cache key of the upstream sub-DAG for the given output port.
    * The payload uses sorted keys and is hashed with SHA-256.
    */
  def computeCacheKey(
      plan: PhysicalPlan,
      target: GlobalPortIdentity
  ): CacheKey = {
    val subDag = collectUpstream(plan, target)
    val payload = buildPayload(subDag.nodes, subDag.links, target)
    val json = objectMapper.writeValueAsString(payload)
    CacheKey(json, sha256Hex(json))
  }

  /**
    * Whether two cache keys identify the same upstream computation.
    *
    * The hash is compared first; on a hash match the full JSON is compared as
    * well. This keeps the match safe against a hash collision: if two different
    * computations ever produced the same hash, their JSON would still differ, so
    * they are reported as different and a cached result is never reused for a port
    * it was not computed from.
    */
  def sameComputation(a: CacheKey, b: CacheKey): Boolean =
    a.hash == b.hash && a.json == b.json

  private case class SubDag(nodes: Set[PhysicalOp], links: Set[PhysicalLink])

  /**
    * Collect all operators and links reachable upstream of the target port's operator.
    * This is limited to the connected component feeding the target.
    */
  private def collectUpstream(plan: PhysicalPlan, target: GlobalPortIdentity): SubDag = {
    val visitedOps = mutable.Set(target.opId)
    val visitedLinks = mutable.Set[PhysicalLink]()
    val queue = mutable.Queue(target.opId)

    while (queue.nonEmpty) {
      val current = queue.dequeue()
      val upstreamLinks = plan.getUpstreamPhysicalLinks(current)
      upstreamLinks.foreach(link => {
        visitedLinks.add(link)
        if (!visitedOps.contains(link.fromOpId)) {
          visitedOps.add(link.fromOpId)
          queue.enqueue(link.fromOpId)
        }
      })
    }

    SubDag(
      nodes = visitedOps.map(plan.getOperator).toSet,
      links = visitedLinks.toSet.filter(link =>
        visitedOps.contains(link.fromOpId) && visitedOps.contains(link.toOpId)
      )
    )
  }

  /**
    * Build the JSON payload describing the sub-DAG:
    *  - target port
    *  - sorted nodes with exec info and schemas
    *  - sorted edges
    */
  private def buildPayload(
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

  private val objectMapper: ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
    mapper.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)
    mapper
  }

  private def sha256Hex(value: String): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8))
    bytes.map("%02x".format(_)).mkString
  }

  /**
    * Serialize op init info deterministically using its proto string.
    */
  private def serializeOpExec(opExecInitInfo: OpExecInitInfo): ObjectNode = {
    val n = objectMapper.createObjectNode()
    // Use the proto string for a stable, deterministic representation.
    n.put("protoString", opExecInitInfo.asMessage.toProtoString)
    n
  }
}
