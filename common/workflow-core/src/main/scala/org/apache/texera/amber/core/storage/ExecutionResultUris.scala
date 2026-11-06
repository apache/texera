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

package org.apache.texera.amber.core.storage

import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, OperatorIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.OPERATOR_PORT_EXECUTIONS
import org.jooq.DSLContext

import java.net.URI
import scala.jdk.CollectionConverters._

/**
  * The result URIs an execution recorded in operator_port_executions.
  */
object ExecutionResultUris {
  private def context: DSLContext =
    SqlServer
      .getInstance()
      .createDSLContext()

  def getResultUrisByExecutionId(eid: ExecutionIdentity): List[URI] = {
    context
      .select(OPERATOR_PORT_EXECUTIONS.RESULT_URI)
      .from(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
      .fetchInto(classOf[String])
      .asScala
      .toList
      .filter(uri => uri != null && uri.nonEmpty)
      .map(URI.create)
  }

  /**
    * This method is mainly used for frontend requests. Given a logicalOpId and an outputPortId of an execution,
    * this method finds the URI for a globalPortId that both: 1. matches the logicalOpId and outputPortId, and
    * 2. is an external port. Currently the lookup is O(n), where n is the number of globalPortIds for this execution.
    * TODO: Optimize the lookup once the frontend also has information about physical operators.
    */
  def getResultUriByLogicalPortId(
      eid: ExecutionIdentity,
      opId: OperatorIdentity,
      portId: PortIdentity
  ): Option[URI] = {
    def isMatchingExternalPortURI(uri: URI): Boolean = {
      val components = VFSURIFactory.decodeURI(uri)
      components.globalPortId.exists { globalPortId =>
        !globalPortId.portId.internal &&
        globalPortId.opId.logicalOpId == opId &&
        globalPortId.portId == portId &&
        components.resourceType == VFSResourceType.RESULT
      }
    }

    val urisOfEid: List[URI] =
      context
        .select(OPERATOR_PORT_EXECUTIONS.RESULT_URI)
        .from(OPERATOR_PORT_EXECUTIONS)
        .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid.id.toInt))
        .fetchInto(classOf[String])
        .asScala
        .toList
        .map(URI.create)

    urisOfEid.find(isMatchingExternalPortURI)
  }
}
