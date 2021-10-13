package edu.uci.ics.texera.web.resource.dashboard.workflow

import com.fasterxml.jackson.databind.ObjectMapper
import com.flipkart.zjsonpatch.JsonPatch
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{WORKFLOW, WORKFLOW_VERSION}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.WorkflowDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.Workflow
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowVersionResource.{
  VersionEntry,
  applyPatch,
  context,
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

  private def applyPatch(versions: List[VersionEntry], workflow: Workflow): Workflow = {
    // loop all versions and apply the patch
    val mapper = new ObjectMapper()
    for (patch <- versions) {
      workflow.setContent(
        JsonPatch
          .apply(mapper.readTree(patch.content), mapper.readTree(workflow.getContent))
          .toString
      )
    }
    // the checked out version is persisted to disk
    workflow
  }
  case class VersionEntry(
      vId: UInteger,
      creationTime: Timestamp,
      content: String
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
      context
        .select(WORKFLOW_VERSION.VID, WORKFLOW_VERSION.CREATION_TIME, WORKFLOW_VERSION.CONTENT)
        .from(WORKFLOW_VERSION)
        .leftJoin(WORKFLOW)
        .on(WORKFLOW_VERSION.WID.eq(WORKFLOW.WID))
        .where(WORKFLOW_VERSION.WID.eq(wid))
        .fetchInto(classOf[VersionEntry])
        .toList
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
        .fetchInto(classOf[VersionEntry])
        .toList
      // apply patch
      val currentWorkflow = workflowDao.fetchOneByWid(wid)
      // return particular version of the workflow
      val res: Workflow = applyPatch(versionEntries.reverse, currentWorkflow)
      res
    }
  }
}
