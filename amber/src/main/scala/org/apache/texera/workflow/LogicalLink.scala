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

package org.apache.texera.workflow

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import com.fasterxml.jackson.databind.JsonNode
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.PortIdentity

object LogicalLink {

  // Reads an OperatorIdentity from either the plain-string shape the
  // frontend emits (`"op-A"`) or the object shape Jackson emits for an
  // OperatorIdentity (`{"id":"op-A"}`). Keep in sync with the
  // workflow-compiling-service LogicalLink.readOperatorIdentity, which is
  // intentionally identical.
  private def readOperatorIdentity(node: JsonNode, fieldName: String): OperatorIdentity = {
    if (node == null || node.isNull) {
      OperatorIdentity(null)
    } else if (node.isTextual) {
      OperatorIdentity(node.asText())
    } else if (node.isObject) {
      val idNode = node.get("id")
      if (idNode == null || idNode.isNull) {
        OperatorIdentity(null)
      } else if (idNode.isTextual) {
        OperatorIdentity(idNode.asText())
      } else {
        throw new IllegalArgumentException(
          s"LogicalLink $fieldName.id must be a string or null, but was ${idNode.getNodeType}"
        )
      }
    } else {
      throw new IllegalArgumentException(
        s"LogicalLink $fieldName must be a string or an object with an id field, but was ${node.getNodeType}"
      )
    }
  }
}

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

  def this(
      fromOpId: String,
      fromPortId: PortIdentity,
      toOpId: String,
      toPortId: PortIdentity
  ) = {
    this(OperatorIdentity(fromOpId), fromPortId, OperatorIdentity(toOpId), toPortId)
  }

  @JsonCreator
  def this(
      @JsonProperty("fromOpId") fromOpId: JsonNode,
      fromPortId: PortIdentity,
      @JsonProperty("toOpId") toOpId: JsonNode,
      toPortId: PortIdentity
  ) = {
    this(
      LogicalLink.readOperatorIdentity(fromOpId, "fromOpId"),
      fromPortId,
      LogicalLink.readOperatorIdentity(toOpId, "toOpId"),
      toPortId
    )
  }
}
