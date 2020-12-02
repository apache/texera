package edu.uci.ics.texera.web.resource.dashboard

import java.util

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{WORKFLOW, WORKFLOW_OF_USER}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{WorkflowDao, WorkflowOfUserDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{Workflow, WorkflowOfUser}
import edu.uci.ics.texera.web.resource.auth.UserResource
import io.dropwizard.jersey.sessions.Session
import javax.servlet.http.HttpSession
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import org.glassfish.jersey.media.multipart.FormDataParam
import org.jooq.types.UInteger

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
  @Path("/get") def getUserWorkflow(@Session session: HttpSession): util.List[Workflow] = {
    val user = UserResource.getUser(session)
    if (user == null) return new util.ArrayList[Workflow]() {}
    getWorkflowsByUser(user.getUserID)
  }

  /**
    * This method handles the client request to get a specific workflow to be displayed
    * at current design, it only takes the workflowID and searches within the database for the matching workflow
    * for future design, it should also take userID as an parameter.
    *
    * @param workflowID workflow id, which serves as the primary key in the UserWorkflow database
    * @param session    HttpSession
    * @return a json string representing an savedWorkflow
    */
  @GET
  @Path("/get/{workflowID}") def getWorkflow(
      @PathParam("workflowID") workflowID: UInteger,
      @Session session: HttpSession
  ): Workflow = workflowDao.fetchOneByWid(workflowID)

  /**
    * This method persists the workflow into database
    *
    * @param session HttpSession
    * @param wfId    an UInteger to identify a workflow
    * @param name    a String, workflow's name
    * @param content a String, workflow's content
    * @return Workflow, which contains the generated wfID if not provided
    */
  @POST
  @Path("/save-workflow")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def saveWorkflow(
      @Session session: HttpSession,
      @FormDataParam("wfId") wfId: UInteger,
      @FormDataParam("name") name: String,
      @FormDataParam("content") content: String
  ): Workflow = {
    val user = UserResource.getUser(session)
    if (user == null) return null
    if (wfId != null) return updateWorkflow(wfId, name, content)
    val workflow = insertWorkflowToDataBase(name, content)
    workflowOfUserDao.insert(new WorkflowOfUser(user.getUserID, workflow.getWid))
    workflow
  }

  /**
    * update table workflow set content = @param "content" where wid = @param "workflowId"
    *
    * @param workflowId an UInteger to identify workflow
    * @param name       a String, workflow's name, can be updated
    * @param content    a String, workflow's content, can be updated
    * @return Workflow
    */
  private def updateWorkflow(workflowId: UInteger, name: String, content: String): Workflow = {
    SqlServer.createDSLContext
      .update(WORKFLOW)
      .set(WORKFLOW.NAME, name)
      .set(WORKFLOW.CONTENT, content)
      .where(WORKFLOW.WID.eq(workflowId))
      .execute
    workflowDao.fetchOneByWid(workflowId)
  }

  /**
    * This private method will be used to insert a non existing workflow into the database
    * There is no request handler that utilize this method yet.
    *
    * @param name    a String, workflow's name
    * @param content a String, workflow's content
    * @return Workflow
    */
  private def insertWorkflowToDataBase(name: String, content: String): Workflow = {
    val workflow = new Workflow
    workflow.setName(name)
    workflow.setContent(content)
    workflowDao.insert(workflow)
    workflow
  }

  /**
    * select * from table workflow where workflowID is @param "workflowID"
    *
    * @param userId an UInteger to identify user
    * @return Workflow
    */
  private def getWorkflowsByUser(userId: UInteger): util.List[Workflow] = {
    SqlServer.createDSLContext
      .select()
      .from(WORKFLOW)
      .join(WORKFLOW_OF_USER)
      .on(WORKFLOW_OF_USER.WID.eq(WORKFLOW.WID))
      .where(WORKFLOW_OF_USER.UID.eq(userId))
      .fetchInto(classOf[Workflow])
  }
}
