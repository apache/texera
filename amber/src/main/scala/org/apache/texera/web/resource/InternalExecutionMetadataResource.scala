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

package org.apache.texera.web.resource

import io.dropwizard.auth.Auth
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.auth.SessionUser
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource
import org.apache.texera.web.service.ExecutionsMetadataPersistService

import java.net.URI
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType

case class CreateExecutionRequest(
    workflowId: Long,
    uid: Option[Integer],
    executionName: String,
    environmentVersion: String,
    computingUnitId: Integer
)

case class CreateExecutionResponse(eid: Long)

case class RuntimeStatsUriRequest(workflowId: Long, uri: String)

case class OperatorConsoleUriRequest(operatorId: String, uri: String)

case class PortResultUriRequest(globalPortId: String, uri: String)

case class ResultUriResponse(uri: String)

case class LatestExecutionResponse(eid: Int)

/**
  * Internal HTTP endpoints that the dashboard service exposes so a computing unit can perform
  * execution-metadata operations without holding Postgres credentials (issue #5011).
  *
  * These endpoints run on the dashboard service, where `SqlServer` is initialized, so the companion
  * methods they delegate to take their direct-DB branch and never recurse back to the remote client.
  */
@Path("/internal/execution-metadata")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class InternalExecutionMetadataResource {

  @POST
  @Path("/create")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def createExecution(
      request: CreateExecutionRequest,
      @Auth user: SessionUser
  ): CreateExecutionResponse = {
    val eid = ExecutionsMetadataPersistService.insertNewExecution(
      WorkflowIdentity(request.workflowId),
      request.uid,
      request.executionName,
      request.environmentVersion,
      request.computingUnitId
    )
    CreateExecutionResponse(eid.id.toLong)
  }

  @PUT
  @Path("/{eid}/runtime-stats-uri")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def updateRuntimeStatsUri(
      @PathParam("eid") eid: Long,
      request: RuntimeStatsUriRequest,
      @Auth user: SessionUser
  ): Unit = {
    WorkflowExecutionsResource.updateRuntimeStatsUri(request.workflowId, eid, new URI(request.uri))
  }

  @POST
  @Path("/{eid}/operator-console")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def insertOperatorConsoleUri(
      @PathParam("eid") eid: Long,
      request: OperatorConsoleUriRequest,
      @Auth user: SessionUser
  ): Unit = {
    WorkflowExecutionsResource.insertOperatorExecutions(
      eid,
      request.operatorId,
      new URI(request.uri)
    )
  }

  @POST
  @Path("/{eid}/port-result")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def insertPortResultUri(
      @PathParam("eid") eid: Long,
      request: PortResultUriRequest,
      @Auth user: SessionUser
  ): Unit = {
    WorkflowExecutionsResource.insertOperatorPortResultUriSerialized(
      ExecutionIdentity(eid),
      request.globalPortId,
      new URI(request.uri)
    )
  }

  @GET
  @Path("/{eid}/port-result")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def getResultUri(
      @PathParam("eid") eid: Long,
      @QueryParam("opId") opId: String,
      @QueryParam("portId") portId: Int,
      @QueryParam("internal") internal: Boolean,
      @Auth user: SessionUser
  ): ResultUriResponse = {
    WorkflowExecutionsResource
      .getResultUriByLogicalPortId(
        ExecutionIdentity(eid),
        OperatorIdentity(opId),
        PortIdentity(portId, internal)
      )
      .map(uri => ResultUriResponse(uri.toString))
      .getOrElse(throw new NotFoundException(s"No result URI found for execution $eid"))
  }

  @GET
  @Path("/latest")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def getLatestExecutionId(
      @QueryParam("wid") wid: Integer,
      @QueryParam("cuid") cuid: Integer,
      @Auth user: SessionUser
  ): LatestExecutionResponse = {
    WorkflowExecutionsResource
      .getLatestExecutionID(wid, cuid)
      .map(eid => LatestExecutionResponse(eid.intValue()))
      .getOrElse(throw new NotFoundException(s"No execution found for workflow $wid"))
  }
}
