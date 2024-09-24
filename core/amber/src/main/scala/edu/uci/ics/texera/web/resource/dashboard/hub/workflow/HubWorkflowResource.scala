package edu.uci.ics.texera.web.resource.dashboard.hub.workflow

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{User, Workflow}

import java.util
import javax.ws.rs._
import javax.ws.rs.core.MediaType

import org.jooq.types.UInteger

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/hub/workflow")
class HubWorkflowResource {
  final private lazy val context = SqlServer.createDSLContext()

  @GET
  @Path("/list")
  def getWorkflowList: util.List[Workflow] = {
    context
      .select()
      .from(WORKFLOW)
      .fetchInto(classOf[Workflow])
  }

  @GET
  @Path("/count")
  def getWorkflowCount: Integer = {
    context.selectCount
      .from(WORKFLOW)
      .fetchOne(0, classOf[Integer])
  }

  @GET
  @Path("/owner_user")
  def getOwnerUser(@QueryParam("wid") wid: UInteger): User = {
    context
      .select(
        USER.UID,
        USER.NAME,
        USER.EMAIL,
        USER.PASSWORD,
        USER.GOOGLE_ID,
        USER.ROLE,
        USER.GOOGLE_AVATAR
      )
      .from(WORKFLOW_OF_USER)
      .join(USER)
      .on(WORKFLOW_OF_USER.UID.eq(USER.UID))
      .where(WORKFLOW_OF_USER.WID.eq(wid))
      .fetchOneInto(classOf[User])
  }

  @GET
  @Path("/workflow_name")
  def getWorkflowName(@QueryParam("wid") wid: UInteger): String = {
    context
      .select(
        WORKFLOW.NAME
      )
      .from(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .fetchOneInto(classOf[String])
  }
}
