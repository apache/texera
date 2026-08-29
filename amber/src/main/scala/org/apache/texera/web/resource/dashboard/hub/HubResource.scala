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

package org.apache.texera.web.resource.dashboard.hub

import io.dropwizard.auth.Auth
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.ActionEnum
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import org.apache.texera.web.resource.dashboard.VersionedResourceTables
import org.apache.texera.web.resource.dashboard.hub.ActionType.{Clone, Like, Unlike, View}
import org.apache.texera.web.resource.dashboard.hub.EntityTables._
import org.apache.texera.web.resource.dashboard.hub.HubResource._
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowResource.{
  DashboardWorkflow,
  baseWorkflowSelect,
  mapWorkflowEntries
}
import org.jooq.impl.DSL
import org.jooq.{Record, Table, TableField}

import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{Context, MediaType}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

object HubResource {

  // Represents an entity reference for general-purpose batch APIs.
  // Used by: isLikedHelper, recordLikeAction, getCounts, userAccess
  case class UserRequest(entityId: Integer, entityType: EntityType)

  // Extends UserRequest by adding userId, used for view tracking.
  // Used by: postView
  case class ViewRequest(entityId: Integer, userId: Integer, entityType: EntityType)

  // Response format indicating whether a given entity is liked by the user.
  // Returned by: isLiked (which calls isLikedHelper), and by isLikedHelper directly.
  case class LikedResponse(
      entityId: Integer,
      entityType: EntityType,
      isLiked: Boolean
  )

  // Response containing all user IDs with access to a specific entity.
  // Returned by: userAccess endpoint
  case class AccessResponse(
      entityType: EntityType,
      entityId: Integer,
      userIds: java.util.List[Integer]
  )

  // Contains aggregated counts (view/like/clone) for a given entity.
  // Returned by: getCounts endpoint
  case class CountResponse(
      entityId: Integer,
      entityType: EntityType,
      counts: java.util.Map[ActionType, Int]
  )

  private def context =
    SqlServer
      .getInstance()
      .createDSLContext()

  final private val ipv4Pattern: Pattern = Pattern.compile(
    "^([0-9]{1,3}\\.){3}[0-9]{1,3}$"
  )

  /**
    * Checks if a given user has liked a specific entity.
    *
    * @param userId The ID of the user.
    * @param entityId The ID of the entity.
    * @param entityType The type of entity being checked (must be validated).
    * @return `true` if the user has liked the entity, otherwise `false`.
    */
  def isLikedHelper(
      userId: Integer,
      entityIds: java.util.List[Integer],
      entityTypes: java.util.List[EntityType]
  ): java.util.List[LikedResponse] = {
    val reqs: List[UserRequest] =
      entityTypes.asScala
        .zip(entityIds.asScala)
        .map { case (etype, id) => UserRequest(id, etype) }
        .toList

    val buffer = ListBuffer[LikedResponse]()
    reqs
      .groupBy(_.entityType)
      .foreach {
        case (etype, groupReqs) =>
          val tbl = EntityTables(etype).like
          val ids = groupReqs.map(_.entityId)

          val likedSet: Set[Int] = context
            .select(tbl.idColumn)
            .from(tbl.table)
            .where(tbl.uidColumn.eq(userId))
            .and(tbl.idColumn.in(ids: _*))
            .fetch()
            .asScala
            .map(r => r.get(tbl.idColumn).intValue())
            .toSet

          groupReqs.foreach { req =>
            val flag = likedSet.contains(req.entityId.intValue())
            buffer += LikedResponse(req.entityId, etype, flag)
          }
      }

    buffer.toList.asJava
  }

  /**
    * Records a user's action in the system.
    *
    * @param request The HTTP request object to extract the user's IP address.
    * @param userId The ID of the user performing the action (default is 0 for anonymous users).
    * @param entityId The ID of the entity associated with the action.
    * @param entityType The type of entity being acted upon (validated before processing).
    * @param action The action performed by the user ("like", "unlike", "view", "clone").
    */
  def recordUserAction(
      request: HttpServletRequest,
      userId: Integer = Integer.valueOf(0),
      entityId: Integer,
      entityType: EntityType,
      action: ActionType
  ): Unit = {
    val userIp = request.getRemoteAddr
    val actionEnum = ActionEnum.values().find(_.getLiteral.equalsIgnoreCase(action.value)).get

    val query = context
      .insertInto(USER_ACTION)
      .set(USER_ACTION.UID, userId)
      .set(USER_ACTION.RESOURCE_ID, entityId)
      .set(USER_ACTION.RESOURCE_TYPE, entityType.value)
      .set(USER_ACTION.ACTION, actionEnum)

    if (ipv4Pattern.matcher(userIp).matches()) {
      query.set(USER_ACTION.IP, userIp)
    }

    query.execute()
  }

  /**
    * Records a user's like or unlike action for a given entity.
    *
    * @param request The HTTP request object to extract the user's IP address.
    * @param userRequest An object containing entityId, userId, and entityType.
    * @param isLike A boolean flag indicating whether the action is a like (`true`) or unlike (`false`).
    * @return `true` if the like/unlike action was recorded successfully, otherwise `false`.
    */
  def recordLikeAction(
      request: HttpServletRequest,
      userId: Integer,
      userRequest: UserRequest,
      isLike: Boolean
  ): Boolean = {
    val (entityId, entityType) =
      (userRequest.entityId, userRequest.entityType)
    val entityTables = EntityTables(entityType).like
    val (table, uidColumn, idColumn) =
      (entityTables.table, entityTables.uidColumn, entityTables.idColumn)

    val likedResponses = isLikedHelper(
      userId,
      List(entityId).asJava,
      List(entityType).asJava
    ).asScala
    val alreadyLiked = likedResponses.headOption.exists(_.isLiked)

    if (isLike && !alreadyLiked) {
      context
        .insertInto(table)
        .set(uidColumn, userId)
        .set(idColumn, entityId)
        .execute()

      recordUserAction(request, userId, entityId, entityType, Like)
      true
    } else if (!isLike && alreadyLiked) {
      context
        .deleteFrom(table)
        .where(uidColumn.eq(userId).and(idColumn.eq(entityId)))
        .execute()

      recordUserAction(request, userId, entityId, entityType, Unlike)
      true
    } else {
      false
    }
  }

  /**
    * Records a user's clone action for a given entity.
    *
    * @param request The HTTP request object to extract the user's IP address.
    * @param userId The ID of the user performing the clone action.
    * @param entityId The ID of the entity being cloned.
    * @param entityType The type of entity being cloned (must be validated).
    */
  def recordCloneAction(
      request: HttpServletRequest,
      userId: Integer,
      entityId: Integer,
      entityType: EntityType
  ): Unit = {

    val entityTables = CloneTable(entityType)
    val (table, uidColumn, idColumn) =
      (entityTables.table, entityTables.uidColumn, entityTables.idColumn)

    recordUserAction(request, userId, entityId, entityType, Clone)

    val existingCloneRecord = context
      .selectFrom(table)
      .where(uidColumn.eq(userId))
      .and(idColumn.eq(entityId))
      .fetchOne()

    if (existingCloneRecord == null) {
      context
        .insertInto(table)
        .set(uidColumn, userId)
        .set(idColumn, entityId)
        .execute()
    }
  }

  def fetchDashboardWorkflowsByWids(wids: Seq[Integer], uid: Integer): List[DashboardWorkflow] = {
    if (wids.isEmpty) {
      return List.empty[DashboardWorkflow]
    }

    val records = baseWorkflowSelect()
      .where(WORKFLOW.WID.in(wids: _*))
      .groupBy(
        WORKFLOW.WID,
        WORKFLOW.NAME,
        WORKFLOW.DESCRIPTION,
        WORKFLOW.CREATION_TIME,
        WORKFLOW.LAST_MODIFIED_TIME,
        WORKFLOW_USER_ACCESS.PRIVILEGE,
        WORKFLOW_OF_USER.UID,
        USER.NAME
      )
      .fetch()

    mapWorkflowEntries(records, uid)
  }

  /**
    * Hydrates ids of one LakeFS-backed resource into hub entries. Unsizable resources are
    * dropped; ids are de-duplicated because the access join can match one twice.
    */
  def fetchDashboardVersionedResourcesByIds(
      tables: VersionedResourceTables[_ <: Record, _],
      ids: Seq[Integer],
      uid: Integer
  ): List[DashboardClickableFileEntry] = {
    if (ids.isEmpty) {
      return List.empty[DashboardClickableFileEntry]
    }

    val records = context
      .select()
      .from(tables.joinWithAccessAndOwner(None))
      .where(tables.idColumn.in(ids: _*))
      .groupBy(
        tables.idColumn,
        tables.nameColumn,
        tables.descriptionColumn,
        tables.ownerUidColumn,
        USER.NAME,
        tables.access.idColumn,
        tables.access.uidColumn,
        USER.UID
      )
      .fetch()

    records.asScala
      .flatMap(record => tables.hydrate(record, uid))
      .toList
      .distinctBy(_._1)
      .map(_._2)
  }
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/hub")
class HubResource {
  private def context =
    SqlServer
      .getInstance()
      .createDSLContext()

  @GET
  @Path("/count")
  def getCount(@QueryParam("entityType") entityType: EntityType): Integer = {
    val entityTables = EntityTables(entityType).base
    val (table, isPublicColumn) = (entityTables.table, entityTables.isPublicColumn)

    context
      .selectCount()
      .from(table)
      .where(isPublicColumn.eq(true))
      .fetchOne(0, classOf[Integer])
  }

  @GET
  @Path("/isLiked")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def isLiked(
      @Auth user: SessionUser,
      @QueryParam("entityId") entityIds: java.util.List[Integer],
      @QueryParam("entityType") entityTypes: java.util.List[EntityType]
  ): java.util.List[LikedResponse] = {
    isLikedHelper(user.getUid, entityIds, entityTypes)
  }

  @POST
  @Path("/like")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def postLike(
      @Auth user: SessionUser,
      @Context request: HttpServletRequest,
      likeRequest: UserRequest
  ): Boolean = {
    recordLikeAction(request, user.getUid, likeRequest, isLike = true)
  }

  @POST
  @Path("/unlike")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def postUnlike(
      @Auth user: SessionUser,
      @Context request: HttpServletRequest,
      unlikeRequest: UserRequest
  ): Boolean = {
    recordLikeAction(request, user.getUid, unlikeRequest, isLike = false)
  }

  @POST
  @Path("/view")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def postView(
      @Context request: HttpServletRequest,
      viewRequest: ViewRequest
  ): Int = {

    val (entityID, userId, entityType) =
      (viewRequest.entityId, viewRequest.userId, viewRequest.entityType)

    val entityTables = ViewCountTable(entityType)
    val (table, idColumn, viewCountColumn) =
      (entityTables.table, entityTables.idColumn, entityTables.viewCountColumn)

    val record = context
      .insertInto(table)
      .set(idColumn, entityID)
      .set(viewCountColumn, Integer.valueOf(1))
      .onDuplicateKeyUpdate()
      .set(viewCountColumn, viewCountColumn.add(1))
      .returning(viewCountColumn)
      .fetchOne()

    recordUserAction(request, userId, entityID, entityType, View)

    record.get(viewCountColumn)
  }

  /**
    * Unified endpoint to fetch the top N (here N = 8) public entities for a given entity type,
    * grouped by specified action types, with optional user context.
    *
    * @param entityType   The EntityType enum value (Workflow, Dataset) to query.
    * @param actionTypes  Optional list of ActionType enums to include (Like, Clone).
    *                     If omitted or empty, defaults to [Like, Clone].
    * @param uid          Optional user ID (Integer) for user-specific context.
    *                     If null or -1, no per-user flags are applied.
    * @param limit        Optional maximum number of items to return per action type.
    *                     Must be > 0; defaults to 8 if not provided or invalid.
    * @return             A Map from each actionType.value (e.g. "like", "clone")
    *                     to a List of DashboardClickableFileEntry containing the top 8
    *                     public entities of that type.
    */
  @GET
  @Path("/getTops")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getTops(
      @QueryParam("entityType") entityType: EntityType,
      @QueryParam("actionTypes") actionTypes: java.util.List[ActionType],
      @QueryParam("uid") uid: Integer,
      @QueryParam("limit") limit: Integer
  ): java.util.Map[String, java.util.List[DashboardClickableFileEntry]] = {
    val tableSet = EntityTables(entityType)
    val baseTable = tableSet.base
    val isPublicColumn = baseTable.isPublicColumn
    val baseIdColumn = baseTable.idColumn
    val topN: Int = Option(limit).filter(_ > 0).map(_.intValue).getOrElse(8)

    val currentUid: Integer =
      if (uid == null || uid == -1) null
      else Integer.valueOf(uid)

    val types: Seq[ActionType] =
      if (actionTypes != null && !actionTypes.isEmpty)
        actionTypes.asScala.toList.distinct
      else
        Seq(ActionType.Like, ActionType.Clone)

    val result: Map[String, java.util.List[DashboardClickableFileEntry]] =
      types.map { act =>
        val rankedBy: Option[(Table[_], TableField[_, Integer])] = act match {
          case ActionType.Like =>
            val lt = tableSet.like
            Some((lt.table, lt.idColumn))
          case ActionType.Clone =>
            tableSet.cloneTable.map(ct => (ct.table, ct.idColumn))
          case other =>
            throw new BadRequestException(
              s"Unsupported actionType: '$other'. Supported: [like, clone]"
            )
        }

        val topIds: Seq[Integer] = rankedBy.toSeq.flatMap {
          case (table, idColumn) =>
            context
              .select(idColumn)
              .from(table)
              .join(baseTable.table)
              .on(idColumn.eq(baseIdColumn))
              .where(isPublicColumn.eq(true))
              .groupBy(idColumn)
              .orderBy(DSL.count(idColumn).desc())
              .limit(topN)
              .fetchInto(classOf[Integer])
              .asScala
              .toSeq
        }

        val entries: Seq[DashboardClickableFileEntry] =
          tableSet.versionedResource match {
            case Some(versionedResource) =>
              fetchDashboardVersionedResourcesByIds(versionedResource, topIds, currentUid)
            case None =>
              entityType match {
                case EntityType.Workflow =>
                  fetchDashboardWorkflowsByWids(topIds, currentUid).map { w =>
                    DashboardClickableFileEntry(
                      resourceType = entityType.value,
                      workflow = Some(w)
                    )
                  }
                case other =>
                  throw new BadRequestException(s"getTops is not supported for '$other'")
              }
          }

        act.value -> entries.toList.asJava
      }.toMap

    result.asJava
  }

  /**
    * Batch endpoint to fetch counts for one or more entities, optionally filtered by action types.
    *
    * Example requests:
    *   // All counts for two entities:
    *   // GET /hub/counts?
    *   //     entityType=workflow&entityId=123&
    *   //     entityType=dataset&entityId=456
    *
    *   // Only "view" and "like" counts for the same pair:
    *   // GET /hub/counts?
    *   //     entityType=workflow&entityId=123&
    *   //     entityType=dataset&entityId=456&
    *   //     actionType=view&actionType=like
    *
    * @param entityTypes   List of entity types to query (enum EntityType), e.g. [Workflow, Dataset].
    * @param entityIds     Parallel list of entity IDs, must be the same length as entityTypes.
    * @param actionTypes   (Optional) List of action types to include (enum ActionType).
    *                      Supported values: View, Like, Clone, Unlike. If empty or null, all actions are returned.
    * @return              A list of CountResponse objects, one per (entityType, entityId) pair,
    *                      each containing the counts for the requested actions.
    * @throws javax.ws.rs.BadRequestException if entityTypes or entityIds are missing, empty, mismatched in length,
    *         or if actionTypes contains an unsupported value.
    */
  @GET
  @Path("/counts")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getCounts(
      @QueryParam("entityType") entityTypes: java.util.List[EntityType],
      @QueryParam("entityId") entityIds: java.util.List[Integer],
      @QueryParam("actionType") actionTypes: java.util.List[ActionType]
  ): java.util.List[CountResponse] = {
    if (
      entityTypes == null || entityIds == null || entityTypes.isEmpty || entityTypes
        .size() != entityIds.size()
    )
      throw new BadRequestException(
        "Both 'entityType' and 'entityId' query parameters must be provided, and lists must have equal length."
      )

    val reqs: List[UserRequest] = entityTypes.asScala
      .zip(entityIds.asScala)
      .map {
        case (etype, id) => UserRequest(id, etype)
      }
      .toList

    val requestedActions: Seq[ActionType] =
      if (actionTypes != null && !actionTypes.isEmpty)
        actionTypes.asScala.toList.distinct
      else
        Seq(ActionType.View, ActionType.Like, ActionType.Clone)

    val grouped: Map[EntityType, Seq[Integer]] =
      reqs.groupBy(_.entityType).view.mapValues(_.map(_.entityId)).toMap

    val buffer = ListBuffer[CountResponse]()

    grouped.foreach {
      case (etype, ids) =>
        val tableSet = EntityTables(etype)
        val viewTbl = tableSet.viewCount
        val viewMap: Map[Int, Int] =
          if (requestedActions.contains(ActionType.View)) {
            val raw = context
              .select(viewTbl.idColumn, viewTbl.viewCountColumn)
              .from(viewTbl.table)
              .where(viewTbl.idColumn.in(ids: _*))
              .fetchMap(viewTbl.idColumn, viewTbl.viewCountColumn)
              .asScala
              .map { case (k, v) => k.intValue() -> v.intValue() }
              .toMap

            val missing = ids.filterNot(id => raw.contains(id.intValue()))

            missing.foreach { id =>
              context
                .insertInto(viewTbl.table)
                .set(viewTbl.idColumn, id)
                .set(viewTbl.viewCountColumn, Integer.valueOf(0))
                .onDuplicateKeyIgnore()
                .execute()
            }

            raw ++ missing.map(id => id.intValue() -> 0).toMap
          } else Map.empty

        val likeTbl = tableSet.like
        val likeMap: Map[Int, Int] =
          if (requestedActions.contains(ActionType.Like)) {
            context
              .select(likeTbl.idColumn, DSL.count().`as`("cnt"))
              .from(likeTbl.table)
              .where(likeTbl.idColumn.in(ids: _*))
              .groupBy(likeTbl.idColumn)
              .fetch()
              .asScala
              .map { r =>
                r.get(likeTbl.idColumn).intValue() ->
                  r.get("cnt", classOf[Integer]).intValue()
              }
              .toMap
          } else Map.empty

        val cloneMap: Map[Int, Int] =
          tableSet.cloneTable
            .filter(_ => requestedActions.contains(ActionType.Clone))
            .map { cloneTbl =>
              context
                .select(cloneTbl.idColumn, DSL.count().`as`("cnt"))
                .from(cloneTbl.table)
                .where(cloneTbl.idColumn.in(ids: _*))
                .groupBy(cloneTbl.idColumn)
                .fetch()
                .asScala
                .map { r =>
                  r.get(cloneTbl.idColumn).intValue() ->
                    r.get("cnt", classOf[Integer]).intValue()
                }
                .toMap
            }
            .getOrElse(Map.empty)

        reqs.filter(_.entityType == etype).foreach { req =>
          val key = req.entityId.intValue()
          val counts = scala.collection.mutable.Map[ActionType, Int]()
          if (requestedActions.contains(ActionType.View))
            counts(ActionType.View) = viewMap.getOrElse(key, 0)
          if (requestedActions.contains(ActionType.Like))
            counts(ActionType.Like) = likeMap.getOrElse(key, 0)
          if (requestedActions.contains(ActionType.Clone))
            counts(ActionType.Clone) = cloneMap.getOrElse(key, 0)

          buffer += CountResponse(req.entityId, etype, counts.asJava)
        }
    }

    buffer.toList.asJava
  }

  /**
    * Batch-fetches the list of user IDs who have access rights for one or more entities.
    * Supports multiple entityType/entityId pairs in a single request.
    *
    * @param entityTypes List of entity types (e.g. Workflow, Dataset) matching the entityIds.
    * @param entityIds   List of entity IDs matching the entityTypes.
    * @return A list of AccessResponse objects, each containing:
    *                     - entityType: the resource type
    *                     - entityId: the resource ID
    *                     - userIds:  the list of user IDs with access to that resource
    */
  @GET
  @Path("/user-access")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def userAccess(
      @QueryParam("entityType") entityTypes: java.util.List[EntityType],
      @QueryParam("entityId") entityIds: java.util.List[Integer]
  ): java.util.List[AccessResponse] = {
    val reqs =
      entityIds.asScala
        .zip(entityTypes.asScala)
        .map { case (id, etype) => UserRequest(id, etype) }
        .toList

    val responses = ListBuffer[AccessResponse]()
    reqs.groupBy(_.entityType).foreach {
      case (etype, groupReqs) =>
        val access = EntityTables(etype).access
        val (tbl, idCol, uidCol) = (access.table, access.idColumn, access.uidColumn)

        val records = context
          .select(idCol, uidCol)
          .from(tbl)
          .where(idCol.in(groupReqs.map(_.entityId).asJava))
          .fetch()
          .asScala

        val accessMap =
          records
            .groupBy(r => r.get(idCol))
            .map {
              case (id, rs) =>
                id -> rs.map(r => r.get(uidCol)).toList
            }

        groupReqs.map(_.entityId).distinct.foreach { eid =>
          val uids = accessMap.getOrElse(eid, Nil).asJava
          responses += AccessResponse(etype, eid, uids)
        }
    }

    responses.toList.asJava
  }
}
