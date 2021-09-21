package edu.uci.ics.texera.web.resource.dashboard

import com.fasterxml.jackson.databind.ObjectMapper
import com.flipkart.zjsonpatch.{JsonPatch}
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{WORKFLOW, WORKFLOW_VERSION}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{WorkflowDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{Workflow}
import edu.uci.ics.texera.web.resource.auth.UserResource
import edu.uci.ics.texera.web.resource.dashboard.WorkflowResource.context
import io.dropwizard.jersey.sessions.Session
import org.glassfish.jersey.media.multipart.FormDataParam
import org.jooq.types.UInteger

import java.sql.Timestamp
import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
  * This file handles various request related to workflows versions.
  * The details of the mysql tables can be found in /core/scripts/sql/texera_ddl.sql
  */

case class VersionEntry(
    vId: UInteger,
    creationTime: Timestamp,
    content: String
)

object WorkflowVersionResource {
  val context = SqlServer.createDSLContext()
}

@Path("/version")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowVersionResource {

  final private val workflowDao = new WorkflowDao(context.configuration)

  /**
    * This method returns the versions of a workflow given by its ID
    *
    * @param session HttpSession
    * @return versions[]
    */
  @GET
  @Path("/versions/{wid}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveVersionsOfWorkflow(
      @PathParam("wid") wid: UInteger,
      @Session session: HttpSession
  ): List[VersionEntry] = {
    UserResource.getUser(session) match {
      case Some(user) =>
        val versions = context
          .select(WORKFLOW_VERSION.VID, WORKFLOW_VERSION.CREATION_TIME, WORKFLOW_VERSION.CONTENT)
          .from(WORKFLOW_VERSION)
          .leftJoin(WORKFLOW)
          .on(WORKFLOW_VERSION.WID.eq(WORKFLOW.WID))
          .where(WORKFLOW_VERSION.WID.eq(wid))
          .fetch()
        versions
          .map(workflowRecord =>
            VersionEntry(
              workflowRecord.into(WORKFLOW_VERSION).getVid,
              workflowRecord.into(WORKFLOW_VERSION).getCreationTime,
              workflowRecord.into(WORKFLOW_VERSION).getContent
            )
          )
          .toList
      case None => List()
    }
  }

  /**
    * This method returns a particular version of a workflow given the vid and wid
    * first, list the versions of the workflow; second, from the current version(last) apply the differences until the requested version
    * third, return the requested workflow
    * @param wid workflowID of the current workflow the user is working on
    * @param vid versionID of the checked-out version to be computed and returned
    * @param session HttpSession
    * @return workflow of a particular version
    */
  @POST
  @Path("/version")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveWorkflowVersion(
      @FormDataParam("wid") wid: UInteger,
      @FormDataParam("vid") vid: UInteger,
      @Session session: HttpSession
  ): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        if (
          WorkflowAccessResource.hasNoWorkflowAccess(wid, user.getUid) ||
          WorkflowAccessResource.hasNoWorkflowAccessRecord(wid, user.getUid)
        ) {
          Response.status(Response.Status.UNAUTHORIZED).build()
        } else {
          // fetch all versions preceding this
          val versions = context
            .select(WORKFLOW_VERSION.VID, WORKFLOW_VERSION.CREATION_TIME, WORKFLOW_VERSION.CONTENT)
            .from(WORKFLOW_VERSION)
            .leftJoin(WORKFLOW)
            .on(WORKFLOW_VERSION.WID.eq(WORKFLOW.WID))
            .where(WORKFLOW.WID.eq(wid).and(WORKFLOW_VERSION.VID.ge(vid)))
            .fetch()
          // apply patch
          val currentWorkflow = workflowDao.fetchOneByWid(wid)
          val versionEntries = versions
            .map(workflowRecord =>
              VersionEntry(
                workflowRecord.into(WORKFLOW_VERSION).getVid,
                workflowRecord.into(WORKFLOW_VERSION).getCreationTime,
                workflowRecord.into(WORKFLOW_VERSION).getContent
              )
            )
            .toList
          // return result
          val res = applyPatch(versionEntries.reverse, currentWorkflow)
          Response
            .ok(res)
            .build()
        }
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }
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
    return workflow
  }
}
