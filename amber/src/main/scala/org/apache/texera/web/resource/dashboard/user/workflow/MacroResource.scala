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

package org.apache.texera.web.resource.dashboard.user.workflow

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.auth.Auth
import org.apache.texera.amber.operator.macroOp.MacroPortSpec
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, WorkflowKindEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  MacroMetadataDao,
  WorkflowDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  MacroMetadata,
  Workflow,
  WorkflowOfUser,
  WorkflowUserAccess
}
import org.apache.texera.web.resource.dashboard.user.workflow.MacroResource._
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowAccessResource.{
  hasReadAccess,
  hasWriteAccess
}
import org.jooq.{DSLContext, JSONB}

import java.sql.Timestamp
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.jdk.CollectionConverters._

/**
  * REST endpoints for macro definitions. A macro is persisted as a `workflow`
  * row with `kind = MACRO` plus a side row in `macro_metadata` carrying the
  * denormalized port / parameter / palette-display fields.
  *
  * Macros reuse the workflow ACL machinery (`workflow_user_access`), so the
  * standard `WorkflowAccessResource.hasReadAccess` / `hasWriteAccess` apply
  * unchanged here.
  */
object MacroResource {

  private def context: DSLContext = SqlServer.getInstance().createDSLContext()
  private def workflowDao = new WorkflowDao(context.configuration)
  private def workflowOfUserDao = new WorkflowOfUserDao(context.configuration)
  private def workflowUserAccessDao = new WorkflowUserAccessDao(context.configuration)
  private def macroMetadataDao = new MacroMetadataDao(context.configuration)

  // Local mapper for the JSONB columns. The Scala module lets PortSpec and
  // MacroPortSpec round-trip as case classes without extra annotations.
  private val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /** Request body for `POST /macro/create`. */
  case class MacroCreateRequest(
      name: String,
      description: Option[String] = None,
      content: String,
      isPublic: Boolean = false,
      portSpec: PortSpec,
      paramSpec: Option[JsonNode] = None,
      category: Option[String] = None,
      icon: Option[String] = None
  )

  /** Declared external boundary of a macro. */
  case class PortSpec(
      inputs: List[MacroPortSpec] = Nil,
      outputs: List[MacroPortSpec] = Nil
  )

  /** Full response for `POST /macro/create` and `GET /macro/{wid}`. */
  case class MacroDetail(
      wid: Integer,
      name: String,
      description: String,
      content: String,
      creationTime: Timestamp,
      lastModifiedTime: Timestamp,
      isPublic: Boolean,
      portSpec: PortSpec,
      paramSpec: JsonNode,
      category: Option[String],
      icon: Option[String],
      isOwner: Boolean,
      readonly: Boolean
  )

  /**
    * Lightweight row for `GET /macro/list`. `content` is intentionally omitted
    * so the operator palette can render without pulling large LogicalPlan blobs
    * over the wire. `usageCount` is the number of distinct non-macro workflows
    * (visible to the requesting user) whose `content` references this macro
    * by `"macroId":"<wid>"`. Surfaced in the "Your Macros" palette as a small
    * "Nx" chip so users can see at a glance how reusable a macro is.
    */
  case class MacroSummary(
      wid: Integer,
      name: String,
      description: String,
      lastModifiedTime: Timestamp,
      portSpec: PortSpec,
      category: Option[String],
      icon: Option[String],
      usageCount: Int
  )

  /**
    * Per-instance schema returned by `GET /macro/{wid}/schema`. In Phase 1 this
    * holds the port spec only; Phase 2 will populate `params` from promoted
    * parameters declared inside the macro body.
    */
  case class MacroSchema(
      inputs: List[MacroPortSpec],
      outputs: List[MacroPortSpec],
      params: List[JsonNode]
  )

  private def jsonbOf[T](value: T): JSONB =
    JSONB.valueOf(mapper.writeValueAsString(value))

  private def jsonbOfNode(node: JsonNode): JSONB =
    JSONB.valueOf(mapper.writeValueAsString(node))

  private def parsePortSpec(jsonb: JSONB): PortSpec =
    Option(jsonb)
      .map(j => mapper.readValue(j.data(), classOf[PortSpec]))
      .getOrElse(PortSpec())

  private def parseParamSpec(jsonb: JSONB): JsonNode =
    Option(jsonb)
      .map(j => mapper.readTree(j.data()))
      .getOrElse(mapper.createArrayNode())
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/macro")
class MacroResource extends LazyLogging {

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/create")
  def create(req: MacroCreateRequest, @Auth sessionUser: SessionUser): MacroDetail = {
    val user = sessionUser.getUser

    val workflow = new Workflow()
    workflow.setName(req.name)
    workflow.setDescription(req.description.orNull)
    workflow.setContent(req.content)
    workflow.setIsPublic(req.isPublic)
    workflow.setKind(WorkflowKindEnum.MACRO)
    workflowDao.insert(workflow)

    workflowOfUserDao.insert(new WorkflowOfUser(user.getUid, workflow.getWid))
    workflowUserAccessDao.insert(
      new WorkflowUserAccess(user.getUid, workflow.getWid, PrivilegeEnum.WRITE)
    )

    // Seed v1 of the macro so LIVE-mode instances can pin to a concrete vid.
    WorkflowVersionResource.insertVersion(workflow, insertingNewWorkflow = true)

    val metadata = new MacroMetadata(
      workflow.getWid,
      jsonbOf(req.portSpec),
      jsonbOfNode(req.paramSpec.getOrElse(mapper.createArrayNode())),
      req.category.orNull,
      req.icon.orNull
    )
    macroMetadataDao.insert(metadata)

    toDetail(
      workflowDao.fetchOneByWid(workflow.getWid),
      metadata,
      isOwner = true,
      readonly = false
    )
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/list")
  def list(@Auth sessionUser: SessionUser): List[MacroSummary] = {
    val uid = sessionUser.getUser.getUid
    val rows = context
      .selectDistinct(
        WORKFLOW.WID,
        WORKFLOW.NAME,
        WORKFLOW.DESCRIPTION,
        WORKFLOW.LAST_MODIFIED_TIME,
        MACRO_METADATA.PORT_SPEC,
        MACRO_METADATA.CATEGORY,
        MACRO_METADATA.ICON
      )
      .from(WORKFLOW)
      .join(WORKFLOW_USER_ACCESS)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
      .leftJoin(MACRO_METADATA)
      .on(MACRO_METADATA.WID.eq(WORKFLOW.WID))
      .where(WORKFLOW.KIND.eq(WorkflowKindEnum.MACRO))
      .and(WORKFLOW_USER_ACCESS.UID.eq(uid))
      .fetch()

    val usageMap = computeMacroUsage(uid)
    rows.asScala.map { r =>
      MacroSummary(
        r.value1(),
        r.value2(),
        r.value3(),
        r.value4(),
        parsePortSpec(r.value5()),
        Option(r.value6()),
        Option(r.value7()),
        usageMap.getOrElse(r.value1().intValue(), 0)
      )
    }.toList
  }

  /**
    * For each macro the user can see, count the distinct non-macro workflows
    * (also user-visible) whose `content` JSON embeds the macro's wid via
    * `"macroId":"<wid>"`. The regex is robust to whitespace variants Jackson
    * may produce.
    *
    * One pass over the user's non-macro workflows; no per-macro round-trip.
    * Cost = O(workflows × content-length). For typical Texera installs
    * (hundreds of workflows, < 100KB each) this is well under a millisecond.
    */
  private def computeMacroUsage(uid: Integer): Map[Int, Int] = {
    val contents = context
      .selectDistinct(WORKFLOW.WID, WORKFLOW.CONTENT)
      .from(WORKFLOW)
      .join(WORKFLOW_USER_ACCESS)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW.WID))
      .where(WORKFLOW.KIND.ne(WorkflowKindEnum.MACRO))
      .and(WORKFLOW_USER_ACCESS.UID.eq(uid))
      .fetch()
    val macroIdRegex = """"macroId"\s*:\s*"(\d+)"""".r
    val counts = scala.collection.mutable.Map[Int, Int]().withDefaultValue(0)
    for (r <- contents.asScala) {
      val content = r.value2()
      if (content != null) {
        // De-dup within a single workflow: one workflow contributes +1 per
        // distinct macroId it references, not per occurrence. The UI surfaces
        // this as "used in N workflows".
        val widsInThisWorkflow = scala.collection.mutable.Set[Int]()
        for (m <- macroIdRegex.findAllMatchIn(content)) {
          widsInThisWorkflow += m.group(1).toInt
        }
        widsInThisWorkflow.foreach(wid => counts(wid) = counts(wid) + 1)
      }
    }
    counts.toMap
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{wid}")
  def get(@PathParam("wid") wid: Integer, @Auth sessionUser: SessionUser): MacroDetail = {
    val uid = sessionUser.getUser.getUid
    if (!hasReadAccess(wid, uid)) {
      throw new ForbiddenException("No sufficient access privilege.")
    }
    val workflow = Option(workflowDao.fetchOneByWid(wid))
      .filter(_.getKind == WorkflowKindEnum.MACRO)
      .getOrElse(throw new NotFoundException(s"Macro $wid not found"))
    val metadata = Option(macroMetadataDao.fetchOneByWid(wid))
      .getOrElse(throw new NotFoundException(s"Macro $wid metadata missing"))
    toDetail(workflow, metadata, isOwner = isOwner(wid, uid), readonly = !hasWriteAccess(wid, uid))
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{wid}/schema")
  def schema(
      @PathParam("wid") wid: Integer,
      @Auth sessionUser: SessionUser
  ): MacroSchema = {
    val uid = sessionUser.getUser.getUid
    if (!hasReadAccess(wid, uid)) {
      throw new ForbiddenException("No sufficient access privilege.")
    }
    val metadata = Option(macroMetadataDao.fetchOneByWid(wid))
      .getOrElse(throw new NotFoundException(s"Macro $wid metadata missing"))
    val ports = parsePortSpec(metadata.getPortSpec)
    MacroSchema(ports.inputs, ports.outputs, params = Nil)
  }

  /**
    * Returns the macro's serialized body so the frontend can inline it into a
    * parent workflow as a SNAPSHOT instance (`MacroOpDesc.linkMode = SNAPSHOT`),
    * detaching that instance from any future edits to the macro definition.
    */
  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{wid}/snapshot-into-instance")
  def snapshotIntoInstance(
      @PathParam("wid") wid: Integer,
      @Auth sessionUser: SessionUser
  ): String = {
    val uid = sessionUser.getUser.getUid
    if (!hasReadAccess(wid, uid)) {
      throw new ForbiddenException("No sufficient access privilege.")
    }
    Option(workflowDao.fetchOneByWid(wid))
      .filter(_.getKind == WorkflowKindEnum.MACRO)
      .map(_.getContent)
      .getOrElse(throw new NotFoundException(s"Macro $wid not found"))
  }

  private def isOwner(wid: Integer, uid: Integer): Boolean =
    context
      .selectCount()
      .from(WORKFLOW_OF_USER)
      .where(WORKFLOW_OF_USER.WID.eq(wid).and(WORKFLOW_OF_USER.UID.eq(uid)))
      .fetchOne(0, classOf[Integer]) > 0

  private def toDetail(
      workflow: Workflow,
      metadata: MacroMetadata,
      isOwner: Boolean,
      readonly: Boolean
  ): MacroDetail =
    MacroDetail(
      workflow.getWid,
      workflow.getName,
      workflow.getDescription,
      workflow.getContent,
      workflow.getCreationTime,
      workflow.getLastModifiedTime,
      workflow.getIsPublic,
      parsePortSpec(metadata.getPortSpec),
      parseParamSpec(metadata.getParamSpec),
      Option(metadata.getCategory),
      Option(metadata.getIcon),
      isOwner,
      readonly
    )
}
