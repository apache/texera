package edu.uci.ics.texera.web.resource.dashboard

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{WORKFLOW, WORKFLOW_OF_USER}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{WorkflowDao, WorkflowOfUserDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{Workflow, WorkflowOfUser}
import edu.uci.ics.texera.web.resource.auth.UserResource
import io.dropwizard.jersey.sessions.Session
import org.jooq.types.UInteger

import java.util
import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response}

/**
  * This file handles various request related to saved-workflows.
  * It sends mysql queries to the MysqlDB regarding the UserWorkflow Table
  * The details of UserWorkflowTable can be found in /core/scripts/sql/texera_ddl.sql
  */
@Path("/workflow")
@Produces(Array(MediaType.APPLICATION_JSON))
class WorkflowResource {
  final private val workflowDao = new WorkflowDao(SqlServer.createDSLContext.configuration)
  final private val workflowOfUserDao = new WorkflowOfUserDao(
    SqlServer.createDSLContext.configuration
  )

  /**
    * This method returns the current in-session user's workflow list
    *
    * @param session HttpSession
    * @return Workflow[]
    */
  @GET
  @Path("/list")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveWorkflowsBySessionUser(@Session session: HttpSession): util.List[Workflow] = {
    UserResource.getUser(session) match {
      case Some(user) =>
        SqlServer.createDSLContext
          .select()
          .from(WORKFLOW)
          .join(WORKFLOW_OF_USER)
          .on(WORKFLOW_OF_USER.WID.eq(WORKFLOW.WID))
          .where(WORKFLOW_OF_USER.UID.eq(user.getUid))
          .fetchInto(classOf[Workflow])
      case None =>
        new util.ArrayList[Workflow]()
    }
  }

  /**
    * This method handles the client request to get a specific workflow to be displayed
    * at current design, it only takes the workflowID and searches within the database for the matching workflow
    * for future design, it should also take userID as an parameter.
    *
    * @param wid     workflow id, which serves as the primary key in the UserWorkflow database
    * @param session HttpSession
    * @return a json string representing an savedWorkflow
    */
  @GET
  @Path("/{wid}")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def retrieveWorkflow(@PathParam("wid") wid: UInteger, @Session session: HttpSession): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        if (workflowOfUserExists(wid, user.getUid)) {
          Response.ok(workflowDao.fetchOneByWid(wid)).build()
        } else {
          Response.status(Response.Status.BAD_REQUEST).build()
        }
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }

  }

  /**
    * This method persists the workflow into database
    *
    * @param session  HttpSession
    * @param workflow , a workflow
    * @return Workflow, which contains the generated wid if not provided
    */
  @POST
  @Path("/persist")
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  def persistWorkflow(@Session session: HttpSession, workflow: Workflow): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        if (workflowOfUserExists(workflow.getWid, user.getUid)) {
          // when the wid is provided, update the existing workflow
          workflowDao.update(workflow)
        } else {
          // when the wid is not provided, treat it as a new workflow
          workflowDao.insert(workflow)
          workflowOfUserDao.insert(new WorkflowOfUser(user.getUid, workflow.getWid))
        }
        Response.ok(workflowDao.fetchOneByWid(workflow.getWid)).build()
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  /**
    * This method deletes the workflow from database
    *
    * @param session HttpSession
    * @return Response, deleted - 200, not deleted - 304 // TODO: change the error code
    */
  @DELETE
  @Path("/{wid}")
  def deleteWorkflow(@PathParam("wid") wid: UInteger, @Session session: HttpSession): Response = {
    UserResource.getUser(session) match {
      case Some(user) =>
        if (workflowOfUserExists(wid, user.getUid)) {
          workflowDao.deleteById(wid)
          Response.ok().build()
        } else {
          Response.notModified().build()
        }
      case None =>
        Response.status(Response.Status.UNAUTHORIZED).build()
    }
  }

  private def workflowOfUserExists(wid: UInteger, uid: UInteger): Boolean = {
    workflowOfUserDao.existsById(
      SqlServer.createDSLContext
        .newRecord(WORKFLOW_OF_USER.UID, WORKFLOW_OF_USER.WID)
        .values(uid, wid)
    )
  }
}
