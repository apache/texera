package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import com.flipkart.zjsonpatch.{JsonDiff, JsonPatch}
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.Utils.objectMapper
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.dao.jooq.generated.Tables.WORKFLOW_VERSION
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{WorkflowDao, WorkflowVersionDao}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{Workflow, WorkflowVersion}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.{
  DashboardWorkflow,
  assignNewOperatorIds
}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowVersionResource._
import io.dropwizard.auth.Auth
import org.jooq.types.UInteger

import java.sql.Timestamp
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
  * This file handles various request related to workflows versions.
  * The details of the mysql tables can be found in /core/scripts/sql/texera_ddl.sql
  */

object WorkflowVersionResource {
  final private lazy val context = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
    .createDSLContext()
  final private lazy val workflowVersionDao = new WorkflowVersionDao(context.configuration)
  final private lazy val workflowDao = new WorkflowDao(context.configuration)
  // constant to indicate versions should be aggregated if they are within the specified time limit
  private final val AGGREGATE_TIME_LIMIT_MILLSEC =
    AmberConfig.workflowVersionCollapseIntervalInMinutes * 60000
  // list of Json keys in the diff patch that are considered UNimportant
  private final val VERSION_UNIMPORTANCE_RULES = List("/operatorPositions/")
  private final val SNAPSHOT_UNIMPORTANCE_RULES = List("replace")

  /**
    * This function does the check of the difference between the current workflow and its previous version if it exists and inserts a new version
    *
    * @param workflow
    * @param insertingNewWorkflow indicates if the workflow didn't exist before
    */
  def insertVersion(workflow: Workflow, insertingNewWorkflow: Boolean): Unit = {
    val wid = workflow.getWid
    // retrieve current workflow from DB
    val currentWorkflow = workflowDao.fetchOneByWid(wid)
    // if the workflow is new then previous workflow is empty
    val existingWorkflowContent = if (insertingNewWorkflow) "{}" else currentWorkflow.getContent
    // compute diff
    val patch = JsonDiff.asJson(
      objectMapper.readTree(workflow.getContent),
      objectMapper.readTree(existingWorkflowContent)
    )

    // if we are creating a new workflow, even if it is empty, create a new version
    // otherwise, only when there is a diff we would create a new version
    if (insertingNewWorkflow || !patch.isEmpty) {
      insertNewVersion(wid, patch.toString)
    }
  }

  /**
    * This function updates the content of the latest version and inserts a new empty version for the current workflow
    *
    * @param patch to update latest version
    * @param wid
    */
  private def updateLatestVersion(patch: String, wid: UInteger): Unit = {
    // get the latest version to update its content
    val vid = getLatestVersion(wid)
    val workflowVersion = workflowVersionDao.fetchOneByVid(vid)
    workflowVersion.setContent(patch)
    workflowVersionDao.update(workflowVersion)
  }

  /**
    * This function retrieves the latest version of a workflow
    *
    * @param wid
    * @return vid
    */
  def getLatestVersion(wid: UInteger): UInteger = {
    val versions = context
      .select(WORKFLOW_VERSION.VID)
      .from(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(wid))
      .fetchInto(classOf[UInteger])
      .asScala
      .toList
    // for backwards compatibility check, old constructed versions would follow the old design by not saving the current
    // version as an empty delta, so should do the check and create one once
    // TODO should remove the check when all versions in the DB follow latest design
    if (versions.isEmpty) {
      return insertNewVersion(wid).getVid
    }
    versions.max
  }

  /**
    * This function inserts a new version for a workflow
    *
    * @param wid
    */
  def insertNewVersion(wid: UInteger, content: String = "[]"): WorkflowVersion = {
    val workflowVersion = new WorkflowVersion()
    workflowVersion.setContent(content)
    workflowVersion.setWid(wid)
    workflowVersionDao.insert(workflowVersion)
    workflowVersion
  }

  /**
    * This function retrieves the content of versions from a specific workflow in a range
    *
    * @param lowerBound lower bound of the version search range
    * @param UpperBound upper bound of the search range
    * @param wid        workflow id
    * @return a list of contents as strings
    */
  def isSnapshotInRangeUnimportant(
      lowerBound: UInteger,
      UpperBound: UInteger,
      wid: UInteger
  ): Boolean = {
    if (lowerBound == UpperBound) {
      return true
    }
    val contents = context
      .select(WORKFLOW_VERSION.CONTENT)
      .from(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(wid))
      .and(WORKFLOW_VERSION.VID.between(lowerBound).and(UpperBound))
      .fetchInto(classOf[String])
      .asScala
      .toList
    contents.forall(content => !isSnapshotImportant(content))
  }

  /**
    * This function parses the content of the delta to determine if it is positional only
    *
    * @param versionContent
    * @return
    */
  private def isSnapshotImportant(versionContent: String): Boolean = {
    val jsonTreeIterator = objectMapper.readTree(versionContent).iterator()
    while (jsonTreeIterator.hasNext) {
      // if the change(which is marked by the key `path` using the Json patch library
      // doesn't contain any of the specified keywords then it shall be deemed important
      if (
        !SNAPSHOT_UNIMPORTANCE_RULES.exists(jsonTreeIterator.next().path("op").asText().contains)
      ) {
        return true
      }
    }
    false
  }

  /**
    * This function gives a label to each version whether it is significant or not based on a few rules
    * The reason why it is computed AFTER retrieving the list of versions is due to multiple reasons:
    * * 1. minimize the changes to the database.
    * * 2. since the frontend sends persisting workflow request often, we don't want to slow down the
    * insertion to DB because of computing the version's importance especially because the request is
    * async, the versions can quickly become inconsistent if there is delay.
    * * 3. The rules can be changed in the future so we want this logic to be changed flexibly.
    *
    * @param versions the version from DB sorted from latest to earliest
    * @return
    */
  private def encodeVersionImportance(
      currentVersions: List[WorkflowVersion]
  ): List[VersionEntry] = {
    var impEncodedVersions: List[VersionEntry] = List()

    val lastVersion = currentVersions.head
    var lastVersionTime = lastVersion.getCreationTime
    impEncodedVersions = impEncodedVersions :+ VersionEntry(
      lastVersion.getVid,
      lastVersion.getCreationTime,
      lastVersion.getContent,
      true
    ) // the first (latest)
    // version is important even if it is positional
    var versionImportance: Boolean = true
    for (version <- currentVersions.tail) {
      if (
        isWithinTimeLimit(
          lastVersionTime,
          version.getCreationTime
        )
      ) {
        versionImportance = false
      } // try reducing unnecessary check of positional versions
      // because parsing the Json string is expensive
      else {
        lastVersionTime = version.getCreationTime
        versionImportance = isVersionImportant(version.getContent)
      }
      impEncodedVersions = impEncodedVersions :+ VersionEntry(
        version.getVid,
        version.getCreationTime,
        version.getContent,
        versionImportance
      )
    }
    impEncodedVersions
  }

  /**
    * This function determines whether this version is still within the time range of previous versions
    *
    * @param latestTime
    * @param currentVersionTimestamp
    * @return
    */
  private def isWithinTimeLimit(
      latestTime: Timestamp,
      currentVersionTimestamp: Timestamp
  ): Boolean = {
    (latestTime.getTime - currentVersionTimestamp.getTime) < AGGREGATE_TIME_LIMIT_MILLSEC
  }

  /**
    * This function parses the content of the delta to determine if it is positional only
    *
    * @param versionContent
    * @return
    */
  private def isVersionImportant(versionContent: String): Boolean = {
    val jsonTreeIterator = objectMapper.readTree(versionContent).iterator()
    while (jsonTreeIterator.hasNext) {
      // if the change(which is marked by the key `path` using the Json patch library
      // doesn't contain any of the specified keywords then it shall be deemed important
      if (
        !VERSION_UNIMPORTANCE_RULES.exists(jsonTreeIterator.next().path("path").asText().contains)
      ) {
        return true
      }
    }
    false
  }

  /**
    * This function applies all the diff versions to a workflow
    *
    * @param versions list of computed delta in each version
    * @param workflow beginning workflow ( more recent)
    * @return the (old) workflow is computed after applying all the patches
    */
  private def applyPatch(versions: List[WorkflowVersion], workflow: Workflow): Workflow = {
    // loop all versions and apply the patch
    for (patch <- versions) {
      workflow.setContent(
        JsonPatch
          .apply(
            objectMapper.readTree(patch.getContent),
            objectMapper.readTree(workflow.getContent)
          )
          .toString
      )
      workflow.setCreationTime(patch.getCreationTime)
      workflow.setLastModifiedTime(patch.getCreationTime)
    }
    // the checked out version is returned
    workflow
  }

  /**
    * This class is to add version importance encoding to the existing `VersionEntry` from DB
    *
    * @param vId
    * @param creationTime
    * @param content
    * @param importance false is not an important version and true is an important version
    */
  case class VersionEntry(
      vId: UInteger,
      creationTime: Timestamp,
      content: String,
      importance: Boolean
  )

}

@Path("/version")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowVersionResource {

  /**
    * This method returns the versions of a workflow given by its ID
    *
    * @return versions[]
    */
  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{wid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def retrieveVersionsOfWorkflow(
      @PathParam("wid") wid: UInteger,
      @Auth sessionUser: SessionUser
  ): List[VersionEntry] = {
    val user = sessionUser.getUser
    if (!WorkflowAccessResource.hasReadAccess(wid, user.getUid)) {
      List()
    } else {
      encodeVersionImportance(
        context
          .select(WORKFLOW_VERSION.VID, WORKFLOW_VERSION.CREATION_TIME, WORKFLOW_VERSION.CONTENT)
          .from(WORKFLOW_VERSION)
          .where(WORKFLOW_VERSION.WID.eq(wid))
          .orderBy(WORKFLOW_VERSION.CREATION_TIME.desc())
          .fetchInto(classOf[WorkflowVersion])
          .asScala
          .toList
      )
    }
  }

  /**
    * This method returns a particular version of a workflow given the vid and wid
    * first, list the versions of the workflow; second, from the current version(last) apply the differences until the requested version
    * third, return the requested workflow
    *
    * @param wid workflowID of the current workflow the user is working on
    * @param vid versionID of the checked-out version to be computed and returned
    * @return workflow of a particular version
    */
  @GET
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{wid}/{vid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def retrieveWorkflowVersion(
      @PathParam("wid") wid: UInteger,
      @PathParam("vid") vid: UInteger,
      @Auth sessionUser: SessionUser
  ): Workflow = {
    val user = sessionUser.getUser
    if (!WorkflowAccessResource.hasReadAccess(wid, user.getUid)) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else {
      // fetch all versions preceding this
      val versionEntries = context
        .select(WORKFLOW_VERSION.VID, WORKFLOW_VERSION.CREATION_TIME, WORKFLOW_VERSION.CONTENT)
        .from(WORKFLOW_VERSION)
        .where(WORKFLOW_VERSION.WID.eq(wid).and(WORKFLOW_VERSION.VID.ge(vid)))
        .fetchInto(classOf[WorkflowVersion])
        .asScala
        .toList
      // apply patch
      val currentWorkflow = workflowDao.fetchOneByWid(wid)
      // return particular version of the workflow
      val res: Workflow = applyPatch(versionEntries.reverse, currentWorkflow)
      res
    }
  }

  @POST
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/clone/{vid}")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def cloneVersion(
      @PathParam("vid") vid: UInteger,
      @Auth sessionUser: SessionUser,
      requestBody: java.util.Map[String, Int]
  ): UInteger = {
    val displayedVersionId = requestBody.get("displayedVersionId")

    // Fetch the workflow ID (`wid`) associated with the specified version (`vid`)
    val versionRecord = Option(
      context
        .select(WORKFLOW_VERSION.WID)
        .from(WORKFLOW_VERSION)
        .where(WORKFLOW_VERSION.VID.eq(vid))
        .fetchOne()
    ).getOrElse {
      throw new NotFoundException(s"Version ID $vid not found.")
    }
    val wid = versionRecord.get(WORKFLOW_VERSION.WID)
    // Use retrieveWorkflowVersion to get the specified version of the workflow
    val workflowVersion = retrieveWorkflowVersion(wid, vid, sessionUser)
    // Generate a new name for the cloned workflow
    val newWorkflowName = s"${workflowVersion.getName}_v${displayedVersionId}_copy"
    // Create a new workflow based on the retrieved version
    val workflowResource = new WorkflowResource()
    val newWorkflow: DashboardWorkflow =
      try {
        workflowResource.createWorkflow(
          new Workflow(
            newWorkflowName,
            workflowVersion.getDescription,
            null,
            assignNewOperatorIds(workflowVersion.getContent),
            null,
            null,
            0.toByte
          ),
          sessionUser
        )
      } catch {
        case e: Exception =>
          throw new InternalServerErrorException(
            "An error occurred while creating the cloned workflow."
          )
      }
    newWorkflow.workflow.getWid
  }
}
