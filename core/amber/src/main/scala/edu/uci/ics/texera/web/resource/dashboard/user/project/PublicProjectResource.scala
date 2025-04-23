package edu.uci.ics.texera.web.resource.dashboard.user.project

import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.jooq.generated.Tables.{PROJECT, PUBLIC_PROJECT, USER}
import edu.uci.ics.texera.dao.jooq.generated.enums.PrivilegeEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{ProjectUserAccessDao, PublicProjectDao}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{ProjectUserAccess, PublicProject}
import io.dropwizard.auth.Auth
import org.jooq.DSLContext

import java.sql.Timestamp
import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs._

case class DashboardPublicProject(
    pid: Integer,
    name: String,
    owner: String,
    creationTime: Timestamp
) {}

@Path("/public/project")
class PublicProjectResource {

  final private val context: DSLContext = SqlServer
    .getInstance()
    .createDSLContext()
  final private lazy val publicProjectDao = new PublicProjectDao(context.configuration)
  final private val projectUserAccessDao = new ProjectUserAccessDao(context.configuration)

  @GET
  @RolesAllowed(Array("ADMIN"))
  @Path("/type/{pid}")
  def getType(@PathParam("pid") pid: Integer): String = {
    if (publicProjectDao.fetchOneByPid(pid) == null)
      "Private"
    else
      "Public"
  }

  @PUT
  @RolesAllowed(Array("ADMIN"))
  @Path("/public/{pid}")
  def makePublic(@PathParam("pid") pid: Integer, @Auth user: SessionUser): Unit = {
    publicProjectDao.insert(new PublicProject(pid, user.getUid))
  }

  @PUT
  @RolesAllowed(Array("ADMIN"))
  @Path("/private/{pid}")
  def makePrivate(@PathParam("pid") pid: Integer): Unit = {
    publicProjectDao.deleteById(pid)
  }

  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/add")
  def addPublicProjects(checkedList: util.List[Integer], @Auth user: SessionUser): Unit = {
    checkedList.forEach(pid => {
      projectUserAccessDao.merge(
        new ProjectUserAccess(
          user.getUid,
          pid,
          PrivilegeEnum.READ
        )
      )
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/list")
  def listPublicProjects(): util.List[DashboardPublicProject] = {
    context
      .select(PUBLIC_PROJECT.PID, PROJECT.NAME, USER.NAME, PROJECT.CREATION_TIME)
      .from(PUBLIC_PROJECT)
      .leftJoin(PROJECT)
      .on(PUBLIC_PROJECT.PID.eq(PROJECT.PID))
      .leftJoin(USER)
      .on(USER.UID.eq(PUBLIC_PROJECT.UID))
      .fetchInto(classOf[DashboardPublicProject])
  }
}
