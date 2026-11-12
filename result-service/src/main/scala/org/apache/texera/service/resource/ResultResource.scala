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

package org.apache.texera.service.resource

import com.fasterxml.jackson.databind.node.ObjectNode
import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.{
  BadRequestException,
  DefaultValue,
  ForbiddenException,
  GET,
  NotFoundException,
  Path,
  PathParam,
  Produces,
  QueryParam,
  ServiceUnavailableException
}
import org.apache.texera.amber.core.storage.model.VirtualDocument
import org.apache.texera.amber.core.storage.{
  DocumentFactory,
  ExecutionResultUris,
  WarehouseReadGuard,
  WarehouseUnavailableException
}
import org.apache.texera.amber.core.tuple.{Attribute, Schema, Tuple, TupleUtils}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, OperatorIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde
import org.apache.texera.auth.SessionUser
import org.apache.texera.auth.util.WorkflowAccess
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.{
  OPERATOR_PORT_EXECUTIONS,
  WORKFLOW_EXECUTIONS,
  WORKFLOW_VERSION
}
import org.apache.texera.dao.jooq.generated.enums.OutputModeEnum
import org.apache.texera.service.resource.ResultResource._
import org.jooq.DSLContext

import java.net.URI
import scala.jdk.CollectionConverters._

object ResultResource {
  // workflow_executions.status of a run that has ended: COMPLETED, FAILED and KILLED.
  private val FinishedStatuses: java.util.List[java.lang.Short] =
    List[java.lang.Short](3.toShort, 4.toShort, 5.toShort).asJava

  // A page is read into memory in a service everyone shares, so one request must not be able to
  // ask for a whole table. This is far above the rows a result panel shows at once.
  private val MaxPageSize = 1000

  private def context: DSLContext = SqlServer.getInstance().createDSLContext()

  case class LatestExecution(eid: Integer)

  /** One result of an execution. `outputMode` and `rowCount` are null where not recorded. */
  case class PortResult(
      operatorId: String,
      portIndex: Int,
      outputMode: String,
      rowCount: java.lang.Long
  )

  case class ResultPage(table: List[ObjectNode], schema: List[Attribute], rowCount: Long)
}

/**
  * Reads the results of an execution, running or finished. Each read loads the table's current
  * snapshot, so the pages of a running execution are as fresh as what it has committed.
  */
@Path("/results")
@Produces(Array(MediaType.APPLICATION_JSON))
@RolesAllowed(Array("REGULAR", "ADMIN"))
class ResultResource {

  /**
    * The caller's own latest finished execution of a workflow. It names only an execution the
    * caller started, so it needs no access check of its own.
    */
  @GET
  @Path("/workflows/{wid}/latest-execution")
  def getLatestExecution(
      @PathParam("wid") wid: Integer,
      @Auth user: SessionUser
  ): LatestExecution = {
    val eid = context
      .select(WORKFLOW_EXECUTIONS.EID)
      .from(WORKFLOW_EXECUTIONS)
      .join(WORKFLOW_VERSION)
      .on(WORKFLOW_VERSION.VID.eq(WORKFLOW_EXECUTIONS.VID))
      .where(WORKFLOW_VERSION.WID.eq(wid))
      .and(WORKFLOW_EXECUTIONS.UID.eq(user.getUid))
      .and(WORKFLOW_EXECUTIONS.STATUS.in(FinishedStatuses))
      .orderBy(WORKFLOW_EXECUTIONS.EID.desc())
      .limit(1)
      .fetchOneInto(classOf[Integer])
    if (eid == null) {
      throw new NotFoundException(s"you have no finished execution of workflow $wid")
    }
    LatestExecution(eid)
  }

  /** The results of an execution, read from the database alone. */
  @GET
  @Path("/executions/{eid}")
  def getResults(@PathParam("eid") eid: Integer, @Auth user: SessionUser): List[PortResult] = {
    requireReadable(eid, user)
    context
      .select(
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID,
        OPERATOR_PORT_EXECUTIONS.OUTPUT_MODE,
        OPERATOR_PORT_EXECUTIONS.RESULT_COUNT
      )
      .from(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid))
      .and(OPERATOR_PORT_EXECUTIONS.RESULT_URI.ne(""))
      .fetch()
      .asScala
      .toList
      .flatMap { row =>
        val port = GlobalPortIdentitySerde.deserializeFromString(row.value1())
        Option.when(!port.portId.internal) {
          PortResult(
            port.opId.logicalOpId.id,
            port.portId.id,
            Option(row.value2()).map(_.getLiteral).orNull,
            row.value3()
          )
        }
      }
      .sortBy(result => (result.operatorId, result.portIndex))
  }

  /** One page of an operator's result. `pageIndex` starts from 1. */
  @GET
  @Path("/executions/{eid}/operators/{operatorId}/pages")
  def getPage(
      @PathParam("eid") eid: Integer,
      @PathParam("operatorId") operatorId: String,
      @QueryParam("port") @DefaultValue("0") port: Int,
      @QueryParam("pageIndex") @DefaultValue("1") pageIndex: Int,
      @QueryParam("pageSize") @DefaultValue("10") pageSize: Int,
      @QueryParam("columnOffset") @DefaultValue("0") columnOffset: Int,
      @QueryParam("columnLimit") @DefaultValue("2147483647") columnLimit: Int,
      @QueryParam("columnSearch") @DefaultValue("") columnSearch: String,
      @Auth user: SessionUser
  ): ResultPage = {
    requireReadable(eid, user)
    if (
      pageIndex < 1 || pageSize < 1 || pageSize > MaxPageSize || columnOffset < 0 ||
      columnLimit < 0 || pageIndex.toLong * pageSize > Int.MaxValue
    ) {
      throw new BadRequestException("the page or column range is out of bounds")
    }

    val uri = resultUri(eid, operatorId, port)
    val (document, schema) = openResult(uri, operatorId)
    val columns = schema.getAttributeNames
      .filter(_.toLowerCase.contains(columnSearch.toLowerCase))
      .drop(columnOffset)
      .take(columnLimit)
    val from = pageSize * (pageIndex - 1)
    val visualization = isVisualization(eid, uri)
    // A row at a time, so a page holds at most one decoded row: the JSON keeps only a preview of
    // each long value, except in a visualization, which is sent whole.
    val table = document
      .getRange(from, from + pageSize, Some(columns))
      .map(row => TupleUtils.convertTuplesToJson(List(row), visualization).head)
      .toList
    ResultPage(
      table,
      columns.map(schema.getAttribute),
      document.getCount
    )
  }

  /** Per-column statistics of an operator's result, from the table's manifests. */
  @GET
  @Path("/executions/{eid}/operators/{operatorId}/statistics")
  def getStatistics(
      @PathParam("eid") eid: Integer,
      @PathParam("operatorId") operatorId: String,
      @QueryParam("port") @DefaultValue("0") port: Int,
      @Auth user: SessionUser
  ): Map[String, Map[String, Any]] = {
    requireReadable(eid, user)
    openResult(resultUri(eid, operatorId, port), operatorId)._1.getTableStatistics
  }

  /**
    * The caller may read an execution they started, or any execution of a workflow they own or
    * were granted. Being able to see a workflow because it is public is not enough: a public
    * workflow shows what its author published, not what an execution of it read from private
    * datasets. An execution that does not exist gets the same refusal, so ids cannot be probed.
    */
  private def requireReadable(eid: Integer, user: SessionUser): Unit = {
    val execution = context
      .select(WORKFLOW_VERSION.WID, WORKFLOW_EXECUTIONS.UID)
      .from(WORKFLOW_EXECUTIONS)
      .join(WORKFLOW_VERSION)
      .on(WORKFLOW_VERSION.VID.eq(WORKFLOW_EXECUTIONS.VID))
      .where(WORKFLOW_EXECUTIONS.EID.eq(eid))
      .fetchOne()
    val readable = execution != null && (
      execution.value2() == user.getUid ||
        WorkflowAccess.hasGrantedAccess(execution.value1(), user.getUid)
    )
    if (!readable) {
      throw new ForbiddenException(s"you cannot read the results of execution $eid")
    }
  }

  private def resultUri(eid: Integer, operatorId: String, port: Int): URI =
    ExecutionResultUris
      .getResultUriByLogicalPortId(
        ExecutionIdentity(eid.longValue()),
        OperatorIdentity(operatorId),
        PortIdentity(port)
      )
      .getOrElse(throw new NotFoundException(s"no result for operator $operatorId, port $port"))

  /**
    * A visualization is HTML in a single cell, so its page is sent whole, as the computing unit's
    * result push sends it. A result recorded before output modes were is not one.
    */
  private def isVisualization(eid: Integer, uri: URI): Boolean =
    context
      .select(OPERATOR_PORT_EXECUTIONS.OUTPUT_MODE)
      .from(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid))
      .and(OPERATOR_PORT_EXECUTIONS.RESULT_URI.eq(uri.toString))
      .fetchOne(OPERATOR_PORT_EXECUTIONS.OUTPUT_MODE) == OutputModeEnum.SINGLE_SNAPSHOT

  private def openResult(uri: URI, operatorId: String): (VirtualDocument[Tuple], Schema) =
    try {
      WarehouseReadGuard.assertReadable(uri)
      val document = DocumentFactory.openDocument(uri)
      (document._1.asInstanceOf[VirtualDocument[Tuple]], document._2.get)
    } catch {
      case e: WarehouseUnavailableException => throw new ServiceUnavailableException(e.getMessage)
      case e: IllegalArgumentException if e.getMessage.contains("No storage is found") =>
        throw new NotFoundException(s"the result of operator $operatorId is no longer stored")
    }
}
