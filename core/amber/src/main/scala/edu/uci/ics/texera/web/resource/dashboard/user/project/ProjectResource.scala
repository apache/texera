package edu.uci.ics.texera.web.resource.dashboard.user.project

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.Tables._
import edu.uci.ics.texera.web.model.jooq.generated.enums.ProjectUserAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  FileOfProjectDao,
  ProjectDao,
  ProjectUserAccessDao,
  WorkflowOfProjectDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos._
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource.DashboardFile
import edu.uci.ics.texera.web.resource.dashboard.user.project.ProjectResource._
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowAccessResource.hasReadAccess
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflow

import io.dropwizard.auth.Auth
import org.apache.commons.lang3.StringUtils
import org.jooq.types.UInteger
import java.sql.Timestamp
import java.util
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
  * This file handles various request related to projects.
  * It sends mysql queries to the MysqlDB regarding the 'user_project',
  * 'workflow_of_project', and 'file_of_project' Tables
  * The details of these tables can be found in /core/scripts/sql/texera_ddl.sql
  */

object ProjectResource {
  final private lazy val context = SqlServer.createDSLContext()
  final private lazy val userProjectDao = new ProjectDao(context.configuration)
  final private lazy val workflowOfProjectDao = new WorkflowOfProjectDao(context.configuration)
  final private lazy val fileOfProjectDao = new FileOfProjectDao(context.configuration)
  final private lazy val projectUserAccessDao = new ProjectUserAccessDao(context.configuration)

  /**
    * This method is used to insert any CSV files created from ResultExportService
    * handleCSVRequest function into all project(s) that the workflow belongs to.
    *
    * No insertion occurs if the workflow does not belong to any projects.
    *
    * @param uid user ID
    * @param wid workflow ID
    * @param fileName name of exported file
    * @return String containing status of adding exported file to project(s)
    */
  def addExportedFileToProject(uid: UInteger, wid: UInteger, fileName: String): String = {
    // get map of PIDs and project names
    val pidMap = context
      .select(WORKFLOW_OF_PROJECT.PID, PROJECT.NAME)
      .from(WORKFLOW_OF_PROJECT)
      .leftJoin(PROJECT)
      .on(WORKFLOW_OF_PROJECT.PID.eq(PROJECT.PID))
      .where(WORKFLOW_OF_PROJECT.WID.eq(wid))
      .fetch()
      .intoMap(WORKFLOW_OF_PROJECT.PID, PROJECT.NAME)

    if (pidMap.size() > 0) { // workflow belongs to project(s)
      // get fid using fileName & cast to UInteger
      val fid = context
        .select(FILE.FID)
        .from(FILE)
        .where(FILE.OWNER_UID.eq(uid).and(FILE.NAME.eq(fileName)))
        .fetchOneInto(FILE)
        .getFid

      // add file to all projects this workflow belongs to
      pidMap
        .keySet()
        .forEach((pid: UInteger) => fileOfProjectDao.insert(new FileOfProject(fid, pid)))

      // generate string for ResultExportResponse
      if (pidMap.size() == 1) {
        s"and added to project: ${pidMap.values().toArray()(0)}"
      } else {
        s"and added to projects: ${pidMap.values().mkString(", ")}"
      }
    } else { // workflow does not belong to a project
      ""
    }
  }

  private def workflowOfProjectExists(wid: UInteger, pid: UInteger): Boolean = {
    workflowOfProjectDao.existsById(
      context
        .newRecord(WORKFLOW_OF_PROJECT.WID, WORKFLOW_OF_PROJECT.PID)
        .values(wid, pid)
    )
  }
  case class DashboardProject(
      pid: UInteger,
      name: String,
      description: String,
      ownerID: UInteger,
      creationTime: Timestamp,
      color: String,
      accessLevel: String
  )
}

@Path("/project")
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Produces(Array(MediaType.APPLICATION_JSON))
class ProjectResource {

  /**
    * This method returns the specified project
    * @param pid project id
    * @return project specified by the project id
    */
  @GET
  @Path("/{pid}")
  def getProject(@PathParam("pid") pid: UInteger): Project = {
    userProjectDao.fetchOneByPid(pid)
  }

  /**
    * This method returns the list of projects owned by the session user.
    * @param user the session user
    * @return a list of projects belonging to owner
    */
  @GET
  @Path("/list")
  def getProjectList(@Auth user: SessionUser): util.List[DashboardProject] = {
    context
      .selectDistinct(
        PROJECT.PID,
        PROJECT.NAME,
        PROJECT.DESCRIPTION,
        PROJECT.OWNER_ID,
        PROJECT.CREATION_TIME,
        PROJECT.COLOR,
        PROJECT_USER_ACCESS.PRIVILEGE
      )
      .from(PROJECT_USER_ACCESS)
      .join(PROJECT)
      .on(PROJECT_USER_ACCESS.PID.eq(PROJECT.PID))
      .where(PROJECT.OWNER_ID.eq(user.getUid).or(PROJECT_USER_ACCESS.UID.eq(user.getUid)))
      .groupBy(PROJECT.PID)
      .fetchInto(classOf[DashboardProject])
  }

  /**
    * This method returns a list of DashboardWorkflow objects, which represents
    * all the workflows that are part of the specified project.
    *
    * @param pid  project ID
    * @param user the session user
    * @return list of DashboardWorkflow objects
    */
  @GET
  @Path("/{pid}/workflows")
  def listProjectWorkflows(
      @PathParam("pid") pid: UInteger,
      @Auth user: SessionUser
  ): List[DashboardWorkflow] = {
    context
      .select()
      .from(WORKFLOW_OF_PROJECT)
      .join(WORKFLOW)
      .on(WORKFLOW.WID.eq(WORKFLOW_OF_PROJECT.WID))
      .join(WORKFLOW_USER_ACCESS)
      .on(WORKFLOW_USER_ACCESS.WID.eq(WORKFLOW_OF_PROJECT.WID))
      .join(WORKFLOW_OF_USER)
      .on(WORKFLOW_OF_USER.WID.eq(WORKFLOW_OF_PROJECT.WID))
      .join(USER)
      .on(USER.UID.eq(WORKFLOW_OF_USER.UID))
      .where(WORKFLOW_OF_PROJECT.PID.eq(pid))
      .fetch()
      .map(workflowRecord =>
        DashboardWorkflow(
          workflowRecord.into(WORKFLOW_OF_USER).getUid.eq(user.getUid),
          workflowRecord
            .into(WORKFLOW_USER_ACCESS)
            .into(classOf[WorkflowUserAccess])
            .getPrivilege
            .toString,
          workflowRecord.into(USER).getName,
          workflowRecord.into(WORKFLOW).into(classOf[Workflow]),
          List()
        )
      )
      .toList
  }

  /**
    * This method returns a list of DashboardFile objects, which represents
    * all the file objects that are part of the specified project.
    * @param pid project ID
    * @return a list of DashboardFile objects
    */
  @GET
  @Path("/{pid}/files")
  def listProjectFiles(
      @PathParam("pid") pid: UInteger
  ): List[DashboardFile] = {
    context
      .select()
      .from(FILE_OF_PROJECT)
      .leftJoin(FILE)
      .on(FILE.FID.eq(FILE_OF_PROJECT.FID))
      .leftJoin(USER)
      .on(USER.UID.eq(FILE.OWNER_UID))
      .where(FILE_OF_PROJECT.PID.eq(pid))
      .fetch()
      .map(fileRecord =>
        DashboardFile(
          fileRecord.into(USER).getName,
          "READ",
          fileRecord.into(FILE).into(classOf[File])
        )
      )
      .toList
  }

  /**
    * This method inserts a new project into the database belonging to the session user
    * and with the specified name.
    * @param user the session user
    * @param name project name
    */
  @POST
  @Path("/create/{name}")
  def createProject(
      @Auth user: SessionUser,
      @PathParam("name") name: String
  ): Project = {
    val project = new Project(null, name, null, user.getUid, null, null)
    try {
      userProjectDao.insert(project)
      projectUserAccessDao.merge(
        new ProjectUserAccess(user.getUid, project.getPid, ProjectUserAccessPrivilege.WRITE)
      )
    } catch {
      case _: Throwable =>
        throw new BadRequestException("Cannot create a new project with provided name.");
    }
    userProjectDao.fetchOneByPid(project.getPid)
  }

  /**
    * This method adds a mapping between the specified workflow to the specified project into the database.
    * @param pid project ID
    * @param wid workflow ID
    */
  @POST
  @Path("/{pid}/workflow/{wid}/add")
  def addWorkflowToProject(
      @PathParam("pid") pid: UInteger,
      @PathParam("wid") wid: UInteger,
      @Auth user: SessionUser
  ): Unit = {
    if (!hasReadAccess(wid, user.getUid)) {
      throw new ForbiddenException("No sufficient access privilege to workflow.")
    }

    if (!workflowOfProjectExists(wid, pid)) {
      workflowOfProjectDao.insert(new WorkflowOfProject(wid, pid))
    }
  }

  /**
    * This method adds a mapping between the specified file to the specified project into the database
    * @param pid project ID
    * @param fid file ID
    */
  @POST
  @Path("/{pid}/user-file/{fid}/add")
  def addFileToProject(
      @PathParam("pid") pid: UInteger,
      @PathParam("fid") fid: UInteger
  ): Unit = {
    fileOfProjectDao.insert(new FileOfProject(fid, pid))
  }

  /**
    * This method updates the project name of the specified, existing project
    * @param pid project ID
    * @param name new name
    */
  @POST
  @Path("/{pid}/rename/{name}")
  def updateProjectName(
      @PathParam("pid") pid: UInteger,
      @PathParam("name") name: String
  ): Unit = {
    val userProject: Project = userProjectDao.fetchOneByPid(pid)
    if (StringUtils.isBlank(name)) {
      throw new BadRequestException("Cannot rename project to empty or blank name.")
    }

    try {
      userProject.setName(name)
      userProjectDao.update(userProject)
    } catch {
      case _: Throwable => throw new BadRequestException("Cannot rename project to provided name.");
    }
  }

  /**
    * This method updates the description of a specified, existing project
    * @param pid project ID
    */
  @POST
  @Path("/{pid}/update/description")
  @Consumes(Array(MediaType.TEXT_PLAIN))
  def updateProjectDescription(
      @PathParam("pid") pid: UInteger,
      description: String
  ): Unit = {
    val userProject: Project = userProjectDao.fetchOneByPid(pid)
    try {
      userProject.setDescription(description)
      userProjectDao.update(userProject)
    } catch {
      case _: Throwable =>
        throw new BadRequestException("Cannot update project description to provided text.");
    }
  }

  /**
    * This method updates a project's color.
    *
    * @param pid id of project to be updated
    * @param colorHex new HEX formatted color to be set
    */
  @POST
  @Path("/{pid}/color/{colorHex}/add")
  def updateProjectColor(
      @PathParam("pid") pid: UInteger,
      @PathParam("colorHex") colorHex: String,
      @Auth sessionUser: SessionUser
  ): Unit = {
    val userProject: Project = userProjectDao.fetchOneByPid(pid)
    if (
      colorHex == null || colorHex.length != 6 && colorHex.length != 3 || !colorHex.matches(
        "^[A-Fa-f0-9]{6}|[A-Fa-f0-9]{3}$"
      )
    ) {
      throw new BadRequestException("Cannot assign invalid HEX format color to project.")
    }

    userProject.setColor(colorHex)
    userProjectDao.update(userProject)
  }

  @POST
  @Path("/{pid}/color/delete")
  def deleteProjectColor(@PathParam("pid") pid: UInteger): Unit = {
    val userProject: Project = userProjectDao.fetchOneByPid(pid)
    userProject.setColor(null)
    userProjectDao.update(userProject)
  }

  /**
    * This method deletes an existing project from the database
    *
    * @param pid projectID
    */
  @DELETE
  @Path("/delete/{pid}")
  def deleteProject(@PathParam("pid") pid: UInteger): Unit = {
    userProjectDao.deleteById(pid)
  }

  /**
    * This method deletes an existing mapping between a workflow and project from
    * the database
    *
    * @param pid project ID
    * @param wid workflow ID
    */
  @DELETE
  @Path("/{pid}/workflow/{wid}/delete")
  def deleteWorkflowFromProject(
      @PathParam("pid") pid: UInteger,
      @PathParam("wid") wid: UInteger
  ): Unit = {
    workflowOfProjectDao.deleteById(
      context.newRecord(WORKFLOW_OF_PROJECT.WID, WORKFLOW_OF_PROJECT.PID).values(wid, pid)
    )
  }

  /**
    * This method deletes an existing mapping between a file and a project from
    * the database
    *
    * @param pid project ID
    * @param fid file ID
    */
  @DELETE
  @Path("/{pid}/user-file/{fid}/delete")
  def deleteFileFromProject(
      @PathParam("pid") pid: UInteger,
      @PathParam("fid") fid: UInteger
  ): Unit = {
    fileOfProjectDao.deleteById(
      context.newRecord(FILE_OF_PROJECT.FID, FILE_OF_PROJECT.PID).values(fid, pid)
    )
  }
}
