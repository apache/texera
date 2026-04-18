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

import io.vertx.core.Vertx
import io.vertx.sqlclient.{Tuple => VxTuple}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{OutputPort, PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.sql.SQLSourceOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.sql.Connection
import scala.collection.mutable

class MySQLSourceOpDesc extends SQLSourceOpDesc {

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        this.operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.source.sql.mysql.MySQLSourceOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "MySQL Source",
      "Read data from a MySQL instance",
      OperatorGroupConstants.DATABASE_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )

  // The JDBC-based SQLSourceOpDesc#querySchema is bypassed for MySQL — the
  // MySQL driver is a non-JDBC Vert.x client. establishConn is still declared
  // on the parent but is unused here.
  override protected def establishConn: Connection = null

  override def updatePort(): Unit = port = if (port.trim().equals("default")) "3306" else port

  override def sourceSchema(): Schema = {
    if (
      host == null || port == null || database == null
      || table == null || username == null || password == null
    ) return null

    updatePort()

    val vertx = Vertx.vertx()
    try {
      val pool = MySQLConnUtil.createPool(vertx, host, port, database, username, password)
      try {
        val rowSet = pool
          .preparedQuery(
            "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns " +
              "WHERE table_schema = ? AND table_name = ? ORDER BY ORDINAL_POSITION"
          )
          .execute(VxTuple.of(database, table))
          .toCompletionStage
          .toCompletableFuture
          .get()

        val attributes = mutable.ListBuffer[Attribute]()
        val it = rowSet.iterator()
        while (it.hasNext) {
          val row = it.next()
          val columnName = row.getString(0)
          val dataType = row.getString(1).toLowerCase
          attributes += new Attribute(columnName, MySQLSourceOpDesc.mapMySQLType(dataType))
        }
        if (attributes.isEmpty) null else Schema(attributes.toList)
      } finally pool.close()
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          this.getClass.getSimpleName + " failed to connect to the database. " + e.getMessage
        )
    } finally vertx.close()
  }
}

object MySQLSourceOpDesc {

  // Maps MySQL `information_schema.columns.DATA_TYPE` strings to Texera AttributeType.
  def mapMySQLType(dataType: String): AttributeType = dataType match {
    case "tinyint" | "smallint" | "mediumint" | "int" | "integer" => AttributeType.INTEGER
    case "bigint"                                                 => AttributeType.LONG
    case "float" | "double" | "real" | "decimal" | "numeric"      => AttributeType.DOUBLE
    case "bit" | "boolean" | "bool"                               => AttributeType.BOOLEAN
    case "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" =>
      AttributeType.BINARY
    case "date" | "time" | "year" | "char" | "varchar" | "tinytext" | "text" |
        "mediumtext" | "longtext" | "enum" | "set" | "json" =>
      AttributeType.STRING
    case "timestamp" | "datetime" => AttributeType.TIMESTAMP
    case other =>
      throw new RuntimeException("MySQLSourceOpDesc: unknown data type: " + other)
  }
}
