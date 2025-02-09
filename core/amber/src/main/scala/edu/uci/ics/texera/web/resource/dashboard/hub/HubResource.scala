package edu.uci.ics.texera.web.resource.dashboard.hub

import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.Tables._
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.Workflow
import HubResource.{
  fetchDashboardWorkflowsByWids,
  getUserLCCount,
  isLikedHelper,
  recordLikeActivity,
  recordUserActivity,
  userRequest,
  validateEntityType
}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflow
import org.jooq.impl.DSL
import org.jooq.types.UInteger

import java.util
import java.util.Collections
import java.util.regex.Pattern
import javax.servlet.http.HttpServletRequest
import javax.ws.rs._
import javax.ws.rs.core.{Context, MediaType}
import scala.jdk.CollectionConverters._
import EntityTables._

object HubResource {
  case class userRequest(entityId: UInteger, userId: UInteger, entityType: String)

  /**
    * Defines the currently accepted resource types.
    * Modify this object to update the valid entity types across the system.
    */
  private object EntityType {
    val allowedTypes: Set[String] = Set("workflow", "dataset")
  }

  final private lazy val context = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
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
  def isLikedHelper(userId: UInteger, workflowId: UInteger, entityType: String): Boolean = {
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
      userId: UInteger = UInteger.valueOf(0),
      entityId: UInteger,
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
      userId: UInteger,
      entityId: UInteger,
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

  /**
    * Retrieves the count of user interactions (likes or clones) for a given entity.
    *
    * @param entityId The ID of the entity whose interaction count is being retrieved.
    * @param entityType The type of entity (must be validated).
    * @param actionType The type of action to count, either "like" or "clone".
    * @return The number of times the entity has been liked or cloned.
    */
  def getUserLCCount(
      entityId: UInteger,
      entityType: String,
      actionType: String
  ): Int = {
    validateEntityType(entityType)

    val entityTables = actionType match {
      case "like"  => LikeTable(entityType)
      case "clone" => CloneTable(entityType)
      case _       => throw new IllegalArgumentException(s"Invalid action type: $actionType")
    }

    val (table, idColumn) = (entityTables.table, entityTables.idColumn)

    context
      .selectCount()
      .from(table)
      .where(idColumn.eq(entityId))
      .fetchOne(0, classOf[Int])
  }

  // todo: refactor api related to landing page
  def fetchDashboardWorkflowsByWids(wids: Seq[UInteger]): util.List[DashboardWorkflow] = {
    if (wids.nonEmpty) {
      context
        .select(
          WORKFLOW.NAME,
          WORKFLOW.DESCRIPTION,
          WORKFLOW.WID,
          WORKFLOW.CREATION_TIME,
          WORKFLOW.LAST_MODIFIED_TIME,
          USER.NAME.as("ownerName"),
          WORKFLOW_OF_USER.UID.as("ownerId")
        )
        .from(WORKFLOW)
        .join(WORKFLOW_OF_USER)
        .on(WORKFLOW.WID.eq(WORKFLOW_OF_USER.WID))
        .join(USER)
        .on(WORKFLOW_OF_USER.UID.eq(USER.UID))
        .where(WORKFLOW.WID.in(wids: _*))
        .fetch()
        .asScala
        .map(record => {
          val workflow = new Workflow(
            record.get(WORKFLOW.NAME),
            record.get(WORKFLOW.DESCRIPTION),
            record.get(WORKFLOW.WID),
            null,
            record.get(WORKFLOW.CREATION_TIME),
            record.get(WORKFLOW.LAST_MODIFIED_TIME),
            null
          )

          DashboardWorkflow(
            isOwner = false,
            accessLevel = "",
            ownerName = record.get("ownerName", classOf[String]),
            workflow = workflow,
            projectIDs = List(),
            ownerId = record.get("ownerId", classOf[UInteger])
          )
        })
        .toList
        .asJava
    } else {
      Collections.emptyList[DashboardWorkflow]()
    }
  }
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/hub")
class HubResource {
  final private lazy val context = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
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
      .where(isPublicColumn.eq(1.toByte))
      .fetchOne(0, classOf[Integer])
  }

  @GET
  @Path("/isLiked")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def isLiked(
      @QueryParam("workflowId") workflowId: UInteger,
      @QueryParam("userId") userId: UInteger,
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

  @GET
  @Path("/likeCount")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getLikeCount(
      @QueryParam("entityId") entityId: UInteger,
      @QueryParam("entityType") entityType: String
  ): Int = {
    getUserLCCount(entityId, entityType, "like")
  }

  @GET
  @Path("/cloneCount")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getCloneCount(
      @QueryParam("entityId") entityId: UInteger,
      @QueryParam("entityType") entityType: String
  ): Int = {
    getUserLCCount(entityId, entityType, "clone")
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

    context
      .insertInto(table)
      .set(idColumn, entityID)
      .set(viewCountColumn, UInteger.valueOf(1))
      .onDuplicateKeyUpdate()
      .set(viewCountColumn, viewCountColumn.add(1))
      .execute()

    recordUserActivity(request, userId, entityID, entityType, "view")

    context
      .select(viewCountColumn)
      .from(table)
      .where(idColumn.eq(entityID))
      .fetchOneInto(classOf[Int])
  }

  @GET
  @Path("/viewCount")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getViewCount(
      @QueryParam("entityId") entityId: UInteger,
      @QueryParam("entityType") entityType: String
  ): Int = {

    validateEntityType(entityType)
    val entityTables = ViewCountTable(entityType)
    val (table, idColumn, viewCountColumn) =
      (entityTables.table, entityTables.idColumn, entityTables.viewCountColumn)

    context
      .insertInto(table)
      .set(idColumn, entityId)
      .set(viewCountColumn, UInteger.valueOf(0))
      .onDuplicateKeyIgnore()
      .execute()

    context
      .select(viewCountColumn)
      .from(table)
      .where(idColumn.eq(entityId))
      .fetchOneInto(classOf[Int])
  }

  @GET
  @Path("/topLovedWorkflows")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getTopLovedWorkflows: util.List[DashboardWorkflow] = {
    val topLovedWorkflowsWids = context
      .select(WORKFLOW_USER_LIKES.WID)
      .from(WORKFLOW_USER_LIKES)
      .join(WORKFLOW)
      .on(WORKFLOW_USER_LIKES.WID.eq(WORKFLOW.WID))
      .where(WORKFLOW.IS_PUBLIC.eq(1.toByte))
      .groupBy(WORKFLOW_USER_LIKES.WID)
      .orderBy(DSL.count(WORKFLOW_USER_LIKES.WID).desc())
      .limit(8)
      .fetchInto(classOf[UInteger])
      .asScala
      .toSeq

    fetchDashboardWorkflowsByWids(topLovedWorkflowsWids)
  }

  @GET
  @Path("/topClonedWorkflows")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getTopClonedWorkflows: util.List[DashboardWorkflow] = {
    val topClonedWorkflowsWids = context
      .select(WORKFLOW_USER_CLONES.WID)
      .from(WORKFLOW_USER_CLONES)
      .join(WORKFLOW)
      .on(WORKFLOW_USER_CLONES.WID.eq(WORKFLOW.WID))
      .where(WORKFLOW.IS_PUBLIC.eq(1.toByte))
      .groupBy(WORKFLOW_USER_CLONES.WID)
      .orderBy(DSL.count(WORKFLOW_USER_CLONES.WID).desc())
      .limit(8)
      .fetchInto(classOf[UInteger])
      .asScala
      .toSeq

    fetchDashboardWorkflowsByWids(topClonedWorkflowsWids)
  }
}
