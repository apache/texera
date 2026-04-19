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

package org.apache.texera.amber.operator.source.sql.mysql

import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.sql.SQLSourceOpDesc

import java.sql.Connection

/**
  * Backward-compatibility shell for the MySQL source operator.
  *
  * The executor and JDBC driver (`mysql-connector-java`, GPLv2 / ASF Category X)
  * were removed for license compliance. The descriptor is kept so that
  * workflows previously persisted with a `MySQLSource` node can still be
  * deserialized and opened in the editor. Attempting to execute the operator
  * raises a clear error.
  */
class MySQLSourceOpDesc extends SQLSourceOpDesc {

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    throw new UnsupportedOperationException(
      "MySQL source operator is no longer executable. The mysql-connector-java " +
        "driver (GPLv2 / ASF Category X) was removed for Apache license compliance. " +
        "This descriptor is retained only so existing workflows can still be loaded. " +
        "Replace this node with PostgreSQLSource or another supported source to run the workflow."
    )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "MySQL Source",
      "[Disabled] MySQL source is no longer available due to license restrictions. " +
        "Replace this operator to run the workflow.",
      OperatorGroupConstants.DATABASE_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )

  // `SQLSourceOpDesc.querySchema` (invoked by the default `sourceSchema`) walks
  // JDBC `DatabaseMetaData`, which is unusable without the removed driver.
  // Return null so the frontend can still open the workflow; the clear error
  // is deferred to `getPhysicalOp`.
  override def sourceSchema(): Schema = null

  override protected def establishConn: Connection = null

  override protected def updatePort(): Unit = ()
}
