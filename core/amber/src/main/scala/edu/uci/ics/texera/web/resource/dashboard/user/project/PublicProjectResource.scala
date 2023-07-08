package edu.uci.ics.texera.web.resource.dashboard.user.project

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{ProjectUserAccessDao, PublicProjectDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{ProjectUserAccess, PublicProject}
import edu.uci.ics.texera.web.model.jooq.generated.Tables.PUBLIC_PROJECT
import edu.uci.ics.texera.web.model.jooq.generated.enums.ProjectUserAccessPrivilege
import io.dropwizard.auth.Auth
import org.jooq.DSLContext
import org.jooq.types.UInteger

import javax.annotation.security.RolesAllowed
import javax.ws.rs._

@Path("/public/project")
class PublicProjectResource {
  final private val context: DSLContext = SqlServer.createDSLContext
  final private lazy val publicProjectDao = new PublicProjectDao(context.configuration)
  final private val projectUserAccessDao = new ProjectUserAccessDao(context.configuration)
  @GET
  @RolesAllowed(Array("ADMIN"))
  @Path("/type/{pid}")
  def getType(@PathParam("pid") pid: UInteger): String = {
    if (publicProjectDao.fetchOneByPid(pid) == null)
      "Private"
    else
      "Public"
  }

  @PUT
  @RolesAllowed(Array("ADMIN"))
  @Path("/public/{pid}")
  def makePublic(@PathParam("pid") pid: UInteger): Unit = {
    publicProjectDao.insert(new PublicProject(pid))
  }

  @PUT
  @RolesAllowed(Array("ADMIN"))
  @Path("/private/{pid}")
  def makePrivate(@PathParam("pid") pid: UInteger): Unit = {
    publicProjectDao.deleteById(pid)
  }

  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/add")
  def add(@Auth user: SessionUser): Unit = {
    context
      .select()
      .from(PUBLIC_PROJECT)
      .fetchInto(classOf[PublicProject])
      .forEach(project => {
        projectUserAccessDao.merge(
          new ProjectUserAccess(
            user.getUid,
            project.getPid,
            ProjectUserAccessPrivilege.READ
          )
        )
      })
  }
}
