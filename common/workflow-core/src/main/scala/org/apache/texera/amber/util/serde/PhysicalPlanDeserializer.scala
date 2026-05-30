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
import com.fasterxml.jackson.databind.`type`.TypeFactory
import com.fasterxml.jackson.databind.{
  DeserializationContext,
  JsonDeserializer,
  JsonNode,
  ObjectMapper
}
import org.apache.texera.amber.core.workflow.{PhysicalLink, PhysicalOp, PhysicalPlan}

import scala.jdk.CollectionConverters._

/**
  * Custom Jackson deserializer for [[PhysicalPlan]].
  *
  * The plan's operators are deserialized by [[PhysicalOpDeserializer]] with EMPTY per-port
  * link lists (links are dropped from the per-port serialized views). This deserializer
  * rebuilds the full plan and then rehydrates each operator's input/output link lists by
  * replaying the plan-level `links`, so the round-tripped plan is structurally identical to
  * the original: every op carries the correct incoming/outgoing links on the correct ports.
  *
  * `addInputLink` / `addOutputLink` are used (not `addLink`) so that schema propagation is
  * NOT re-run: per-port schemas were already restored during operator deserialization.
  */
class PhysicalPlanDeserializer extends JsonDeserializer[PhysicalPlan] {

  override def deserialize(p: JsonParser, ctxt: DeserializationContext): PhysicalPlan = {
    val mapper = p.getCodec.asInstanceOf[ObjectMapper]
    val node: JsonNode = mapper.readTree(p)
    val tf: TypeFactory = mapper.getTypeFactory

    val operatorsNode = node.get("operators")
    val linksNode = node.get("links")

    val operators: Set[PhysicalOp] =
      if (operatorsNode == null || operatorsNode.isNull) Set.empty
      else {
        val setType = tf.constructCollectionType(classOf[java.util.LinkedHashSet[_]], classOf[PhysicalOp])
        val javaSet: java.util.Set[PhysicalOp] = mapper.convertValue(operatorsNode, setType)
        javaSet.asScala.toSet
      }

    val links: Set[PhysicalLink] =
      if (linksNode == null || linksNode.isNull) Set.empty
      else {
        val setType =
          tf.constructCollectionType(classOf[java.util.LinkedHashSet[_]], classOf[PhysicalLink])
        val javaSet: java.util.Set[PhysicalLink] = mapper.convertValue(linksNode, setType)
        javaSet.asScala.toSet
      }

    rebuildLinks(PhysicalPlan(operators, links))
  }

  /**
    * Replays `plan.links` onto each operator's per-port link lists. Operators arrive with
    * empty link lists; we append each link to its source op's output port and its
    * destination op's input port.
    */
  private def rebuildLinks(plan: PhysicalPlan): PhysicalPlan = {
    val opMap = scala.collection.mutable.Map[
      org.apache.texera.amber.core.virtualidentity.PhysicalOpIdentity,
      PhysicalOp
    ]()
    plan.operators.foreach(op => opMap(op.id) = op)

    plan.links.foreach { link =>
      opMap.get(link.fromOpId).foreach(op => opMap(link.fromOpId) = op.addOutputLink(link))
      opMap.get(link.toOpId).foreach(op => opMap(link.toOpId) = op.addInputLink(link))
    }

    plan.copy(operators = opMap.values.toSet, links = plan.links)
  }
}
