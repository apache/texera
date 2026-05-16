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

import com.fasterxml.jackson.databind.JsonNode
import io.dropwizard.auth.Auth
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.tables.daos.WorkflowDao
import org.jooq.{DSLContext, Field, Record}
import org.jooq.impl.DSL

import java.sql.Timestamp
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.jdk.CollectionConverters._

/**
  * Time Machine — full-snapshot version history for workflows.
  *
  * This is a parallel mechanism to WorkflowVersionResource, which stores JSON
  * diffs on persist. Snapshots here are full content captures tagged with the
  * discrete event that produced them (operator added, link removed, etc.).
  *
  * Reads/writes go through plain DSL because the generated jOOQ bindings for
  * workflow_snapshot aren't part of this branch — regenerating jOOQ would be a
  * heavier change than this hackathon scope warrants.
  */
object WorkflowSnapshotResource {

  // raw table / field references (no generated jOOQ bindings for this table)
  private val T = DSL.table(DSL.name("workflow_snapshot"))
  private val SID: Field[Integer] = DSL.field(DSL.name("sid"), classOf[Integer])
  private val WID: Field[Integer] = DSL.field(DSL.name("wid"), classOf[Integer])
  private val UID: Field[Integer] = DSL.field(DSL.name("uid"), classOf[Integer])
  private val SNAPSHOT_VERSION: Field[Integer] =
    DSL.field(DSL.name("snapshot_version"), classOf[Integer])
  private val CONTENT: Field[String] = DSL.field(DSL.name("content"), classOf[String])
  private val CHANGE_TYPE: Field[String] = DSL.field(DSL.name("change_type"), classOf[String])
  private val CHANGE_SUMMARY: Field[String] = DSL.field(DSL.name("change_summary"), classOf[String])
  private val CHANGED_OPERATORS: Field[String] =
    DSL.field(DSL.name("changed_operators"), classOf[String])
  private val SOURCE: Field[String] = DSL.field(DSL.name("source"), classOf[String])
  private val CREATION_TIME: Field[Timestamp] =
    DSL.field(DSL.name("creation_time"), classOf[Timestamp])

  private def context: DSLContext = SqlServer.getInstance().createDSLContext()

  /** Public DTO returned by list endpoint. Lightweight — no full content. */
  case class SnapshotEntry(
      sid: Int,
      wid: Int,
      version: Int,
      changeType: String,
      changeSummary: String,
      changedOperators: List[String],
      source: String,
      uid: Option[Int],
      creationTime: Timestamp
  )

  /** Full snapshot including content. Returned by get-one and revert. */
  case class SnapshotFull(
      sid: Int,
      wid: Int,
      version: Int,
      changeType: String,
      changeSummary: String,
      changedOperators: List[String],
      source: String,
      uid: Option[Int],
      creationTime: Timestamp,
      content: String
  )

  /** Request body for POST /snapshots. */
  case class SnapshotCreateRequest(
      content: String,
      changeType: String,
      changeSummary: String,
      changedOperators: java.util.List[String],
      source: String
  ) {
    def changedOpsList: List[String] =
      Option(changedOperators).map(_.asScala.toList).getOrElse(Nil)
  }

  /**
    * A single operator in a diff. `displayName` is pre-resolved on the
    * server side from `customDisplayName` → `operatorType` → `operatorID`,
    * so the frontend can render it directly without re-parsing the snapshot
    * or relying on Option-field serialization. `operatorType` is also
    * returned separately so the UI can prefer a friendlier schema label
    * (via OperatorMetadataService.userFriendlyName) when one is available.
    */
  case class OperatorDiffEntry(
      operatorID: String,
      operatorType: String,
      customDisplayName: String,
      displayName: String
  )

  case class DiffResult(
      v1: Int,
      v2: Int,
      operatorsAdded: List[OperatorDiffEntry],
      operatorsRemoved: List[OperatorDiffEntry],
      operatorsModified: List[OperatorDiffEntry],
      linksAdded: Int,
      linksRemoved: Int
  )

  private def nextVersion(ctx: DSLContext, wid: Integer): Int = {
    val max = ctx
      .select(DSL.max(SNAPSHOT_VERSION))
      .from(T)
      .where(WID.eq(wid))
      .fetchOne()
    if (max == null || max.value1() == null) 1 else max.value1().intValue() + 1
  }

  private def toEntry(r: Record): SnapshotEntry =
    SnapshotEntry(
      sid = r.get(SID).intValue(),
      wid = r.get(WID).intValue(),
      version = r.get(SNAPSHOT_VERSION).intValue(),
      changeType = r.get(CHANGE_TYPE),
      changeSummary = r.get(CHANGE_SUMMARY),
      changedOperators = parseStringList(r.get(CHANGED_OPERATORS)),
      source = r.get(SOURCE),
      uid = Option(r.get(UID)).map(_.intValue()),
      creationTime = r.get(CREATION_TIME)
    )

  private def toFull(r: Record): SnapshotFull = {
    val e = toEntry(r)
    SnapshotFull(
      sid = e.sid,
      wid = e.wid,
      version = e.version,
      changeType = e.changeType,
      changeSummary = e.changeSummary,
      changedOperators = e.changedOperators,
      source = e.source,
      uid = e.uid,
      creationTime = e.creationTime,
      content = r.get(CONTENT)
    )
  }

  private def parseStringList(raw: String): List[String] = {
    if (raw == null || raw.isEmpty) return Nil
    try {
      val node = objectMapper.readTree(raw)
      if (node.isArray) node.elements().asScala.map(_.asText()).toList else Nil
    } catch {
      case _: Throwable => Nil
    }
  }

  /** Extract operator IDs from a workflow content JSON string. */
  private def extractOperatorIds(content: String): Set[String] = {
    if (content == null || content.isEmpty) return Set.empty
    try {
      val root = objectMapper.readTree(content)
      val ops = root.path("operators")
      if (!ops.isArray) return Set.empty
      ops.elements().asScala.map(_.path("operatorID").asText("")).filter(_.nonEmpty).toSet
    } catch {
      case _: Throwable => Set.empty
    }
  }

  /** Build a `id -> JSON node` map of operators inside a workflow content. */
  private def operatorMap(content: String): Map[String, JsonNode] = {
    if (content == null || content.isEmpty) return Map.empty
    try {
      val ops = objectMapper.readTree(content).path("operators")
      if (!ops.isArray) return Map.empty
      ops.elements().asScala.toList.flatMap { op =>
        val id = op.path("operatorID").asText("")
        if (id.isEmpty) None else Some(id -> op)
      }.toMap
    } catch {
      case _: Throwable => Map.empty
    }
  }

  /** Extract a stable id for each link (source op + port -> target op + port). */
  private def linkIds(content: String): Set[String] = {
    if (content == null || content.isEmpty) return Set.empty
    try {
      val links = objectMapper.readTree(content).path("links")
      if (!links.isArray) return Set.empty
      links.elements().asScala.map { l =>
        val src = l.path("source")
        val tgt = l.path("target")
        s"${src.path("operatorID").asText()}:${src.path("portID").asText()}->" +
          s"${tgt.path("operatorID").asText()}:${tgt.path("portID").asText()}"
      }.toSet
    } catch {
      case _: Throwable => Set.empty
    }
  }

  private def toDiffEntry(id: String, node: JsonNode): OperatorDiffEntry = {
    val opType = node.path("operatorType").asText("")
    val custom = node.path("customDisplayName").asText("")
    val resolved =
      if (custom.nonEmpty) custom
      else if (opType.nonEmpty) opType
      else id
    OperatorDiffEntry(
      operatorID = id,
      operatorType = opType,
      customDisplayName = custom,
      displayName = resolved
    )
  }

  private def computeDiff(v1Content: String, v2Content: String, v1: Int, v2: Int): DiffResult = {
    val ops1 = operatorMap(v1Content)
    val ops2 = operatorMap(v2Content)
    val ids1 = ops1.keySet
    val ids2 = ops2.keySet
    // For added/modified, pull metadata from v2 (the "after" side).
    // For removed, pull from v1 since the operator no longer exists in v2.
    val added = (ids2 -- ids1).toList.sorted.map(id => toDiffEntry(id, ops2(id)))
    val removed = (ids1 -- ids2).toList.sorted.map(id => toDiffEntry(id, ops1(id)))
    val modified = (ids1 intersect ids2).toList
      .filter(id => ops1(id) != ops2(id))
      .sorted
      .map(id => toDiffEntry(id, ops2(id)))
    val l1 = linkIds(v1Content)
    val l2 = linkIds(v2Content)
    DiffResult(
      v1 = v1,
      v2 = v2,
      operatorsAdded = added,
      operatorsRemoved = removed,
      operatorsModified = modified,
      linksAdded = (l2 -- l1).size,
      linksRemoved = (l1 -- l2).size
    )
  }

  private def hasReadAccess(wid: Integer, user: SessionUser): Boolean =
    WorkflowAccessResource.hasReadAccess(wid, user.getUser.getUid)

  private def hasWriteAccess(wid: Integer, user: SessionUser): Boolean =
    WorkflowAccessResource.hasWriteAccess(wid, user.getUser.getUid)
}

@Path("/time-machine")
@Produces(Array(MediaType.APPLICATION_JSON))
@Consumes(Array(MediaType.APPLICATION_JSON))
class WorkflowSnapshotResource {
  import WorkflowSnapshotResource._

  /** List snapshots, newest first. Lightweight (no content). */
  @GET
  @Path("/{wid}/snapshots")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def listSnapshots(
      @PathParam("wid") wid: Integer,
      @Auth sessionUser: SessionUser
  ): java.util.List[SnapshotEntry] = {
    if (!hasReadAccess(wid, sessionUser)) {
      throw new ForbiddenException("No sufficient access privilege.")
    }
    val ctx = context
    ctx
      .select(
        SID,
        WID,
        UID,
        SNAPSHOT_VERSION,
        CHANGE_TYPE,
        CHANGE_SUMMARY,
        CHANGED_OPERATORS,
        SOURCE,
        CREATION_TIME
      )
      .from(T)
      .where(WID.eq(wid))
      .orderBy(SNAPSHOT_VERSION.desc())
      .fetch()
      .asScala
      .map(toEntry)
      .toList
      .asJava
  }

  /** Get one snapshot including full content. Used for preview and revert. */
  @GET
  @Path("/{wid}/snapshots/{sid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def getSnapshot(
      @PathParam("wid") wid: Integer,
      @PathParam("sid") sid: Integer,
      @Auth sessionUser: SessionUser
  ): SnapshotFull = {
    if (!hasReadAccess(wid, sessionUser)) {
      throw new ForbiddenException("No sufficient access privilege.")
    }
    val rec = context
      .select(
        SID,
        WID,
        UID,
        SNAPSHOT_VERSION,
        CHANGE_TYPE,
        CHANGE_SUMMARY,
        CHANGED_OPERATORS,
        SOURCE,
        CREATION_TIME,
        CONTENT
      )
      .from(T)
      .where(WID.eq(wid).and(SID.eq(sid)))
      .fetchOne()
    if (rec == null) throw new NotFoundException(s"Snapshot $sid not found for workflow $wid")
    toFull(rec)
  }

  /** Create a new snapshot for this workflow. */
  @POST
  @Path("/{wid}/snapshots")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def createSnapshot(
      @PathParam("wid") wid: Integer,
      req: SnapshotCreateRequest,
      @Auth sessionUser: SessionUser
  ): SnapshotEntry = {
    if (!hasWriteAccess(wid, sessionUser)) {
      throw new ForbiddenException("No sufficient access privilege.")
    }
    if (req == null || req.content == null || req.content.isEmpty) {
      throw new BadRequestException("Snapshot content is required.")
    }
    val ctx = context
    val v = nextVersion(ctx, wid)
    val changedOpsJson =
      objectMapper.writeValueAsString(req.changedOpsList.asJava)
    val source = Option(req.source).filter(Set("user", "agent").contains).getOrElse("user")
    val uid: Integer = sessionUser.getUser.getUid

    val sid: Integer = ctx
      .insertInto(T)
      .columns(
        WID,
        UID,
        SNAPSHOT_VERSION,
        CONTENT,
        CHANGE_TYPE,
        CHANGE_SUMMARY,
        CHANGED_OPERATORS,
        SOURCE
      )
      .values(
        wid,
        uid,
        Integer.valueOf(v),
        req.content,
        Option(req.changeType).getOrElse("manual_save"),
        Option(req.changeSummary).getOrElse(""),
        changedOpsJson,
        source
      )
      .returning(SID)
      .fetchOne()
      .get(SID)

    val rec = ctx
      .select(
        SID,
        WID,
        UID,
        SNAPSHOT_VERSION,
        CHANGE_TYPE,
        CHANGE_SUMMARY,
        CHANGED_OPERATORS,
        SOURCE,
        CREATION_TIME
      )
      .from(T)
      .where(SID.eq(sid))
      .fetchOne()
    toEntry(rec)
  }

  /**
    * Revert the workflow content to the snapshot. Writes the snapshot's content
    * back into the workflow row and records a new snapshot describing the revert.
    */
  @POST
  @Path("/{wid}/snapshots/{sid}/revert")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def revertSnapshot(
      @PathParam("wid") wid: Integer,
      @PathParam("sid") sid: Integer,
      @Auth sessionUser: SessionUser
  ): SnapshotFull = {
    if (!hasWriteAccess(wid, sessionUser)) {
      throw new ForbiddenException("No sufficient access privilege.")
    }
    val ctx = context
    val target = getSnapshot(wid, sid, sessionUser)

    // overwrite workflow content with the snapshot
    val workflowDao = new WorkflowDao(ctx.configuration)
    val workflow = workflowDao.fetchOneByWid(wid)
    if (workflow == null) throw new NotFoundException(s"Workflow $wid not found")
    workflow.setContent(target.content)
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    workflowDao.update(workflow)

    // record the revert as its own snapshot entry, pointing at the same content
    val v = nextVersion(ctx, wid)
    val uid: Integer = sessionUser.getUser.getUid
    val emptyOps = objectMapper.writeValueAsString(List.empty[String].asJava)
    ctx
      .insertInto(T)
      .columns(
        WID,
        UID,
        SNAPSHOT_VERSION,
        CONTENT,
        CHANGE_TYPE,
        CHANGE_SUMMARY,
        CHANGED_OPERATORS,
        SOURCE
      )
      .values(
        wid,
        uid,
        Integer.valueOf(v),
        target.content,
        "revert",
        s"Reverted to version ${target.version}",
        emptyOps,
        "user"
      )
      .execute()

    target
  }

  /** Diff between two snapshots of the same workflow. */
  @GET
  @Path("/{wid}/diff")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def diffSnapshots(
      @PathParam("wid") wid: Integer,
      @QueryParam("v1") sid1: Integer,
      @QueryParam("v2") sid2: Integer,
      @Auth sessionUser: SessionUser
  ): DiffResult = {
    if (!hasReadAccess(wid, sessionUser)) {
      throw new ForbiddenException("No sufficient access privilege.")
    }
    if (sid1 == null || sid2 == null) {
      throw new BadRequestException("Both v1 and v2 query params (snapshot sids) are required.")
    }
    val a = getSnapshot(wid, sid1, sessionUser)
    val b = getSnapshot(wid, sid2, sessionUser)
    computeDiff(a.content, b.content, a.version, b.version)
  }
}
