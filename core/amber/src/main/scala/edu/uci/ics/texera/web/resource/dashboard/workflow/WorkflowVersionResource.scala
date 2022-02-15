package edu.uci.ics.texera.web.resource.dashboard.workflow

import com.fasterxml.jackson.databind.ObjectMapper
import com.flipkart.zjsonpatch.JsonPatch
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{WORKFLOW, WORKFLOW_VERSION}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.WorkflowDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{Workflow, WorkflowVersion}
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowVersionResource.{
  VersionEntry,
  applyPatch,
  context,
  encodeVersionImportance,
  workflowDao
}
import io.dropwizard.auth.Auth
import org.jooq.types.UInteger

import java.sql.Timestamp
import javax.annotation.security.PermitAll
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
  * This file handles various request related to workflows versions.
  * The details of the mysql tables can be found in /core/scripts/sql/texera_ddl.sql
  */

object WorkflowVersionResource {
  final private lazy val context = SqlServer.createDSLContext()
  final private val workflowDao = new WorkflowDao(context.configuration)
  // constant to indicate versions should be aggregated if they are within the specified time limit
  private final val AGGREGATE_TIME_LIMIT_MILLSEC =
    AmberUtils.amberConfig.getInt("user-sys.version-time-limit-in-minutes") * 60000
  // list of Json keys in the diff patch that are considered UNimportant
  private final val VERSION_UNIMPORTANCE_RULES = List("/operatorPositions/")

  /**
    * This function gives a label to each version whether it is significant or not based on a few rules
    * The reason why it is computed AFTER retrieving the list of versions is due to multiple reasons:
    * * 1. minimize the changes to the database.
    * * 2. since the frontend sends persisting workflow request often, we don't want to slow down the
    * insertion to DB because of computing the version's importance especially because the request is
    * async, the versions can quickly become inconsistent if there is delay.
    * * 3. The rules can be changed in the future so we want this logic to be changed flexibly.
    * @param versions
    * @return
    */
  private def encodeVersionImportance(
      versions: List[WorkflowVersion]
  ): List[VersionEntry] = {
    var impEncodedVersions: List[VersionEntry] = List()
    if (versions.isEmpty) {
      return impEncodedVersions
    }
    val lastVersion = versions.head
    var lastVersionTime = lastVersion.getCreationTime
    impEncodedVersions = impEncodedVersions :+ VersionEntry(
      lastVersion.getVid,
      lastVersion.getCreationTime,
      lastVersion.getContent,
      true
    ) // the first (latest)
    // version is important even if it is positional
    var versionImportance: Boolean = true
    for (version <- versions.init) {
      if (isWithinTimeLimit(lastVersionTime, version.getCreationTime)) {
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
    * @param versionContent
    * @return
    */
  private def isVersionImportant(versionContent: String): Boolean = {
    val mapper = new ObjectMapper()
    val jsonTreeIterator = mapper.readTree(versionContent).iterator()
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
    * @param versions list of computed delta in each version
    * @param workflow beginning workflow ( more recent)
    * @return the (old) workflow is computed after applying all the patches
    */
  private def applyPatch(versions: List[WorkflowVersion], workflow: Workflow): Workflow = {
    // loop all versions and apply the patch
    val mapper = new ObjectMapper()
    for (patch <- versions) {
      workflow.setContent(
        JsonPatch
          .apply(mapper.readTree(patch.getContent), mapper.readTree(workflow.getContent))
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

@PermitAll
@Path("/version")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowVersionResource {

  /**
    * This method returns the versions of a workflow given by its ID
    *
    * @return versions[]
    */
  @GET
  @Path("/{wid}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveVersionsOfWorkflow(
      @PathParam("wid") wid: UInteger,
      @Auth sessionUser: SessionUser
  ): List[VersionEntry] = {
    val user = sessionUser.getUser
    if (
      WorkflowAccessResource.hasNoWorkflowAccess(wid, user.getUid) ||
      WorkflowAccessResource.hasNoWorkflowAccessRecord(wid, user.getUid)
    ) {
      List()
    } else {
      encodeVersionImportance(
        context
          .select(WORKFLOW_VERSION.VID, WORKFLOW_VERSION.CREATION_TIME, WORKFLOW_VERSION.CONTENT)
          .from(WORKFLOW_VERSION)
          .leftJoin(WORKFLOW)
          .on(WORKFLOW_VERSION.WID.eq(WORKFLOW.WID))
          .where(WORKFLOW_VERSION.WID.eq(wid))
          .fetchInto(classOf[WorkflowVersion])
          .toList
          .reverse
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
  @Path("/{wid}/{vid}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveWorkflowVersion(
      @PathParam("wid") wid: UInteger,
      @PathParam("vid") vid: UInteger,
      @Auth sessionUser: SessionUser
  ): Workflow = {
    val user = sessionUser.getUser
    if (
      WorkflowAccessResource.hasNoWorkflowAccess(wid, user.getUid) ||
      WorkflowAccessResource.hasNoWorkflowAccessRecord(wid, user.getUid)
    ) {
      throw new ForbiddenException("No sufficient access privilege.")
    } else {
      // fetch all versions preceding this
      val versionEntries = context
        .select(WORKFLOW_VERSION.VID, WORKFLOW_VERSION.CREATION_TIME, WORKFLOW_VERSION.CONTENT)
        .from(WORKFLOW_VERSION)
        .leftJoin(WORKFLOW)
        .on(WORKFLOW_VERSION.WID.eq(WORKFLOW.WID))
        .where(WORKFLOW.WID.eq(wid).and(WORKFLOW_VERSION.VID.ge(vid)))
        .fetchInto(classOf[WorkflowVersion])
        .toList
      // apply patch
      val currentWorkflow = workflowDao.fetchOneByWid(wid)
      // return particular version of the workflow
      val res: Workflow = applyPatch(versionEntries.reverse, currentWorkflow)
      res
    }
  }
}
