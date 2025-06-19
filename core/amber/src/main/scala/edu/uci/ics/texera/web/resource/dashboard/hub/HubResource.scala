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

package edu.uci.ics.texera.web.resource.dashboard.hub

import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.Tables._
import HubResource.{
  CountRequest,
  CountResponse,
  fetchDashboardDatasetsByDids,
  fetchDashboardWorkflowsByWids,
  isLikedHelper,
  recordLikeActivity,
  recordUserActivity,
  userRequest,
  validateEntityType
}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.{
  DashboardWorkflow,
  baseWorkflowSelect,
  mapWorkflowEntries
}
import org.jooq.impl.DSL

import java.util
import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{Context, MediaType}
import scala.jdk.CollectionConverters._
import EntityTables._
import edu.uci.ics.amber.core.storage.util.LakeFSStorageClient
import edu.uci.ics.texera.dao.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.dao.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS
import edu.uci.ics.texera.dao.jooq.generated.tables.User.USER
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{Dataset, DatasetUserAccess}
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.DashboardClickableFileEntry
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.DashboardDataset

import scala.collection.mutable.ListBuffer

object HubResource {
  case class userRequest(entityId: Integer, userId: Integer, entityType: String)
  case class CountRequest(entityId: Integer, entityType: String)
  case class CountResponse(
      entityId: Integer,
      entityType: String,
      counts: java.util.Map[String, Int]
  )

  /**
    * Defines the currently accepted resource types.
    * Modify this object to update the valid entity types across the system.
    */
  private object EntityType {
    val allowedTypes: Set[String] = Set("workflow", "dataset")
  }

  final private lazy val context = SqlServer
    .getInstance()
    .createDSLContext()

  final private val ipv4Pattern: Pattern = Pattern.compile(
    "^([0-9]{1,3}\\.){3}[0-9]{1,3}$"
  )

  /**
    * Validates whether the provided entity type is allowed.
    *
    * @param entityType The entity type to validate.
    * @throws IllegalArgumentException if the entity type is not in the allowed list.
    */
  def validateEntityType(entityType: String): Unit = {
    if (!EntityType.allowedTypes.contains(entityType)) {
      throw new IllegalArgumentException(
        s"Invalid entity type: $entityType. Allowed types: ${EntityType.allowedTypes.mkString(", ")}."
      )
    }
  }

  /**
    * Checks if a given user has liked a specific entity.
    *
    * @param userId The ID of the user.
    * @param workflowId The ID of the entity.
    * @param entityType The type of entity being checked (must be validated).
    * @return `true` if the user has liked the entity, otherwise `false`.
    */
  def isLikedHelper(userId: Integer, workflowId: Integer, entityType: String): Boolean = {
    validateEntityType(entityType)
    val entityTables = LikeTable(entityType)
    val (table, uidColumn, idColumn) =
      (entityTables.table, entityTables.uidColumn, entityTables.idColumn)

    context
      .selectFrom(table)
      .where(
        uidColumn
          .eq(userId)
          .and(idColumn.eq(workflowId))
      )
      .fetchOne() != null
  }

  /**
    * Records a user's activity in the system.
    *
    * @param request The HTTP request object to extract the user's IP address.
    * @param userId The ID of the user performing the action (default is 0 for anonymous users).
    * @param entityId The ID of the entity associated with the action.
    * @param entityType The type of entity being acted upon (validated before processing).
    * @param action The action performed by the user ("like", "unlike", "view", "clone").
    */
  def recordUserActivity(
      request: HttpServletRequest,
      userId: Integer = Integer.valueOf(0),
      entityId: Integer,
      entityType: String,
      action: String
  ): Unit = {
    validateEntityType(entityType)

    val userIp = request.getRemoteAddr()

    val query = context
      .insertInto(USER_ACTIVITY)
      .set(USER_ACTIVITY.UID, userId)
      .set(USER_ACTIVITY.ID, entityId)
      .set(USER_ACTIVITY.TYPE, entityType)
      .set(USER_ACTIVITY.ACTIVATE, action)

    if (ipv4Pattern.matcher(userIp).matches()) {
      query.set(USER_ACTIVITY.IP, userIp)
    }

    query.execute()
  }

  /**
    * Records a user's like or unlike activity for a given entity.
    *
    * @param request The HTTP request object to extract the user's IP address.
    * @param userRequest An object containing entityId, userId, and entityType.
    * @param isLike A boolean flag indicating whether the action is a like (`true`) or unlike (`false`).
    * @return `true` if the like/unlike action was recorded successfully, otherwise `false`.
    */
  def recordLikeActivity(
      request: HttpServletRequest,
      userRequest: userRequest,
      isLike: Boolean
  ): Boolean = {
    val (entityId, userId, entityType) =
      (userRequest.entityId, userRequest.userId, userRequest.entityType)
    validateEntityType(entityType)
    val entityTables = LikeTable(entityType)
    val (table, uidColumn, idColumn) =
      (entityTables.table, entityTables.uidColumn, entityTables.idColumn)

    val alreadyLiked = isLikedHelper(userId, entityId, entityType)

    if (isLike && !alreadyLiked) {
      context
        .insertInto(table)
        .set(uidColumn, userId)
        .set(idColumn, entityId)
        .execute()

      recordUserActivity(request, userId, entityId, entityType, "like")
      true
    } else if (!isLike && alreadyLiked) {
      context
        .deleteFrom(table)
        .where(uidColumn.eq(userId).and(idColumn.eq(entityId)))
        .execute()

      recordUserActivity(request, userId, entityId, entityType, "unlike")
      true
    } else {
      false
    }
  }

  /**
    * Records a user's clone activity for a given entity.
    *
    * @param request The HTTP request object to extract the user's IP address.
    * @param userId The ID of the user performing the clone action.
    * @param entityId The ID of the entity being cloned.
    * @param entityType The type of entity being cloned (must be validated).
    */
  def recordCloneActivity(
      request: HttpServletRequest,
      userId: Integer,
      entityId: Integer,
      entityType: String
  ): Unit = {

    validateEntityType(entityType)
    val entityTables = CloneTable(entityType)
    val (table, uidColumn, idColumn) =
      (entityTables.table, entityTables.uidColumn, entityTables.idColumn)

    recordUserActivity(request, userId, entityId, entityType, "clone")

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

  def fetchDashboardDatasetsByDids(dids: Seq[Integer], uid: Integer): List[DashboardDataset] = {
    if (dids.isEmpty) {
      return List.empty[DashboardDataset]
    }

    val records = context
      .select()
      .from(
        DATASET
          .leftJoin(DATASET_USER_ACCESS)
          .on(DATASET_USER_ACCESS.DID.eq(DATASET.DID))
          .leftJoin(USER)
          .on(USER.UID.eq(DATASET.OWNER_UID))
      )
      .where(DATASET.DID.in(dids: _*))
      .groupBy(
        DATASET.DID,
        DATASET.NAME,
        DATASET.DESCRIPTION,
        DATASET.OWNER_UID,
        USER.NAME,
        DATASET_USER_ACCESS.DID,
        DATASET_USER_ACCESS.UID,
        USER.UID
      )
      .fetch()

    records.asScala
      .map { record =>
        val dataset = record.into(DATASET).into(classOf[Dataset])
        val datasetAccess = record.into(DATASET_USER_ACCESS).into(classOf[DatasetUserAccess])
        val ownerEmail = record.into(USER).getEmail
        DashboardDataset(
          isOwner = if (uid == null) false else dataset.getOwnerUid == uid,
          dataset = dataset,
          accessPrivilege = datasetAccess.getPrivilege,
          ownerEmail = ownerEmail,
          size = LakeFSStorageClient.retrieveRepositorySize(dataset.getName)
        )
      }
      .toList
      .distinctBy(_.dataset.getDid)
  }
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/hub")
class HubResource {
  final private lazy val context = SqlServer
    .getInstance()
    .createDSLContext()

  @GET
  @Path("/count")
  def getPublishedWorkflowCount(@QueryParam("entityType") entityType: String): Integer = {

    validateEntityType(entityType)
    val entityTables = BaseEntityTable(entityType)
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
      @QueryParam("workflowId") workflowId: Integer,
      @QueryParam("userId") userId: Integer,
      @QueryParam("entityType") entityType: String
  ): Boolean = {
    isLikedHelper(userId, workflowId, entityType)
  }

  @POST
  @Path("/like")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def likeWorkflow(
      @Context request: HttpServletRequest,
      likeRequest: userRequest
  ): Boolean = {
    recordLikeActivity(request, likeRequest, isLike = true)
  }

  @POST
  @Path("/unlike")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def unlikeWorkflow(
      @Context request: HttpServletRequest,
      unlikeRequest: userRequest
  ): Boolean = {
    recordLikeActivity(request, unlikeRequest, isLike = false)
  }

  @POST
  @Path("/view")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  def viewWorkflow(
      @Context request: HttpServletRequest,
      viewRequest: userRequest
  ): Int = {

    val (entityID, userId, entityType) =
      (viewRequest.entityId, viewRequest.userId, viewRequest.entityType)

    validateEntityType(entityType)
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

    recordUserActivity(request, userId, entityID, entityType, "view")

    record.get(viewCountColumn)
  }

  /**
    * Unified endpoint to fetch the top N (here N = 8) entities of given action types (like/clone)
    * for a specified entityType (e.g., "workflow" or "dataset") and optional user ID.
    *
    * @param entityType   The type of entity ("workflow" or "dataset") to query.
    * @param actionTypes  A list of action types to fetch tops for (supported: "like", "clone").
    *                     Example: ?entityType=workflow&actionTypes=like&actionTypes=clone
    * @param uid          Optional user ID for context (e.g., to mark liked/cloned by this user).
    *                     If null or -1, no user-specific context is applied.
    * @return             A map from each actionType to a list of DashboardClickableFileEntry,
    *                     representing the top 8 public entities of that type.
    */
  @GET
  @Path("/getTops")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getTops(
      @QueryParam("entityType") entityType: String,
      @QueryParam("actionTypes") actionTypes: java.util.List[String],
      @QueryParam("uid") uid: Integer
  ): java.util.Map[String, java.util.List[DashboardClickableFileEntry]] = {
    validateEntityType(entityType)

    val baseTable = BaseEntityTable(entityType)
    val isPublicColumn = baseTable.isPublicColumn
    val baseIdColumn = baseTable.idColumn

    val currentUid: Integer =
      if (uid == null || uid == -1) null
      else Integer.valueOf(uid)

    val types: Seq[String] = actionTypes.asScala
      .map(_.toLowerCase)
      .distinct
      .toSeq

    val result: Map[String, java.util.List[DashboardClickableFileEntry]] =
      types.map { act =>
        val (table, idColumn) = act match {
          case "like" =>
            val lt = LikeTable(entityType)
            (lt.table, lt.idColumn)
          case "clone" =>
            val ct = CloneTable(entityType)
            (ct.table, ct.idColumn)
          case other =>
            throw new BadRequestException(
              s"Unsupported actionType: '$other'. Supported: [like, clone]"
            )
        }

        val topIds: Seq[Integer] = context
          .select(idColumn)
          .from(table)
          .join(baseTable.table)
          .on(idColumn.eq(baseIdColumn))
          .where(isPublicColumn.eq(true))
          .groupBy(idColumn)
          .orderBy(DSL.count(idColumn).desc())
          .limit(8)
          .fetchInto(classOf[Integer])
          .asScala
          .toSeq

        val entries: Seq[DashboardClickableFileEntry] =
          if (entityType == "workflow") {
            fetchDashboardWorkflowsByWids(topIds, currentUid).map { w =>
              DashboardClickableFileEntry(
                resourceType = "workflow",
                workflow = Some(w),
                project = None,
                dataset = None
              )
            }
          } else if (entityType == "dataset") {
            fetchDashboardDatasetsByDids(topIds, currentUid).map { d =>
              DashboardClickableFileEntry(
                resourceType = "dataset",
                workflow = None,
                project = None,
                dataset = Some(d)
              )
            }
          } else {
            Seq.empty
          }

        act -> entries.toList.asJava
      }.toMap

    result.asJava
  }

  /**
    * Batch endpoint to fetch counts for one or more entities, optionally filtered by action types.
    *
    * Example requests:
    *   // All counts for two entities:
    *   GET /hub/batch?
    *       entityType=workflow&entityId=123&
    *       entityType=dataset&entityId=456
    *
    *   // Only "view" and "like" counts for the same pair:
    *   GET /hub/batch?
    *       entityType=workflow&entityId=123&
    *       entityType=dataset&entityId=456&
    *       actionType=view&actionType=like
    *
    * @param entityTypes   A list of entity types, e.g. ["workflow","dataset"].
    * @param entityIds     A parallel list of entity IDs, e.g. [123,456].
    *                      Must have the same length as `entityTypes`.
    * @param actionTypes   (Optional) A list of actions to fetch counts for.
    *                      Valid values: "view", "like", "clone".
    *                      If omitted or empty, all three counts are returned.
    * @return              A list of CountResponse objects, one per requested (type,id) pair,
    *                      each containing the counts for the requested actions.
    * @throws BadRequestException if `entityTypes` or `entityIds` are missing,
    *         empty, or of mismatched length, or if any unsupported actionType is provided.
    */
  @GET
  @Path("/counts")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getBatchCounts(
      @QueryParam("entityType") entityTypes: java.util.List[String],
      @QueryParam("entityId") entityIds: java.util.List[Integer],
      @QueryParam("actionType") actionTypes: java.util.List[String]
  ): java.util.List[CountResponse] = {
    if (
      entityTypes == null || entityIds == null || entityTypes.isEmpty || entityTypes
        .size() != entityIds.size()
    )
      throw new BadRequestException(
        "Both 'entityType' and 'entityId' query parameters must be provided, and lists must have equal length."
      )

    val reqs: List[CountRequest] = entityTypes.asScala
      .zip(entityIds.asScala)
      .map {
        case (etype, id) => CountRequest(id, etype)
      }
      .toList

    val allowedActions = Set("view", "like", "clone")
    val requestedActions: Seq[String] =
      if (actionTypes != null && !actionTypes.isEmpty) actionTypes.asScala.toSeq
      else allowedActions.toSeq

    requestedActions.find(a => !allowedActions.contains(a)).foreach { invalid =>
      throw new BadRequestException(
        s"Unsupported actionType: '$invalid'. Supported: ${allowedActions.mkString(", ")}"
      )
    }

    val grouped: Map[String, Seq[Integer]] =
      reqs.groupBy(_.entityType).view.mapValues(_.map(_.entityId)).toMap

    val buffer = ListBuffer[CountResponse]()

    grouped.foreach {
      case (etype, ids) =>
        validateEntityType(etype)

        val viewTbl = ViewCountTable(etype)
        val viewMap: Map[Int, Int] =
          if (requestedActions.contains("view")) {
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

        val likeTbl = LikeTable(etype)
        val likeMap: Map[Int, Int] =
          if (requestedActions.contains("like")) {
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
          if (requestedActions.contains("clone") && etype != "dataset") {
            val cloneTbl = CloneTable(etype)
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
          } else Map.empty

        reqs.filter(_.entityType == etype).foreach { req =>
          val key = req.entityId.intValue()
          val counts = scala.collection.mutable.Map[String, Int]()
          if (requestedActions.contains("view")) counts("view") = viewMap.getOrElse(key, 0)
          if (requestedActions.contains("like")) counts("like") = likeMap.getOrElse(key, 0)
          if (requestedActions.contains("clone")) counts("clone") = cloneMap.getOrElse(key, 0)

          buffer += CountResponse(req.entityId, etype, counts.asJava)
        }
    }

    buffer.toList.asJava
  }
}
