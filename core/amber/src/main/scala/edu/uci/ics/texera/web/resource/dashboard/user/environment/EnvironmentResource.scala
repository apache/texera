package edu.uci.ics.texera.web.resource.dashboard.user.environment

import edu.uci.ics.texera.Utils.withTransaction
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{Dataset, DatasetOfEnvironment, DatasetVersion, Environment}
import edu.uci.ics.texera.web.model.jooq.generated.tables.Environment.ENVIRONMENT
import edu.uci.ics.texera.web.model.jooq.generated.tables.EnvironmentOfWorkflow.ENVIRONMENT_OF_WORKFLOW
import edu.uci.ics.texera.web.model.jooq.generated.tables.DatasetOfEnvironment.DATASET_OF_ENVIRONMENT
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{DatasetDao, DatasetOfEnvironmentDao, DatasetVersionDao, EnvironmentDao, EnvironmentOfWorkflowDao}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.retrieveDatasetVersionFilePaths
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.`type`.DatasetFileDesc
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.{DatasetAccessResource, DatasetResource}
import edu.uci.ics.texera.web.resource.dashboard.user.environment.EnvironmentResource.{DashboardEnvironment, DatasetID, DatasetOfEnvironmentAlreadyExistsMessage, DatasetOfEnvironmentDetails, DatasetOfEnvironmentDoseNotExistMessage, EnvironmentIDs, UserNoPermissionExceptionMessage, WorkflowLink, context, doesDatasetExistInEnvironment, doesUserOwnEnvironment, getEnvironmentByEid, retrieveDatasetsAndVersions, retrieveDatasetsOfEnvironmentFileList, userHasReadAccessToEnvironment, userHasWriteAccessToEnvironment}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowAccessResource
import io.dropwizard.auth.Auth
import org.jooq.DSLContext
import org.jooq.types.UInteger

import java.net.URLDecoder
import java.nio.file.Paths
import javax.annotation.security.RolesAllowed
import javax.ws.rs.core.{MediaType, Response}
import javax.ws.rs.{GET, POST, Path, PathParam, Produces}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.matching.Regex

object EnvironmentResource {
  private val context = SqlServer.createDSLContext()

  def getEnvironmentByWid(ctx: DSLContext, uid: UInteger, wid: UInteger): Environment = {
    val environmentOfWorkflowDao = new EnvironmentOfWorkflowDao(ctx.configuration())
    val environmentOfWorkflow = environmentOfWorkflowDao.fetchByWid(wid)

    if (environmentOfWorkflow == null || environmentOfWorkflow.isEmpty) {
      throw new Exception("No environment associated with this workflow ID")
    }

    getEnvironmentByEid(ctx, environmentOfWorkflow.get(0).getEid)
  }

  def doesWorkflowHaveEnvironment(ctx: DSLContext, wid: UInteger): Boolean = {
    val environmentOfWorkflowDao = new EnvironmentOfWorkflowDao(ctx.configuration())
    val environmentOfWorkflow = environmentOfWorkflowDao.fetchByWid(wid)

    environmentOfWorkflow != null && !environmentOfWorkflow.isEmpty
  }

  def createEnvironment(
      ctx: DSLContext,
      uid: UInteger,
      name: String,
      description: String
  ): Environment = {
    val environment = new Environment()
    environment.setOwnerUid(uid)
    environment.setName(name)
    environment.setDescription(description)

    ctx
      .insertInto(ENVIRONMENT)
      .set(ctx.newRecord(ENVIRONMENT, environment))
      .returning()
      .fetchOne()
      .into(classOf[Environment])
  }

  // return the descriptor of the target file.
  // The filename is passed from the frontend, the did is contained in the filename in the format of /{dataset-name}/{filepath}
  def getEnvironmentDatasetFilePathAndVersion(uid: UInteger, eid: UInteger, fileName: String): DatasetFileDesc = {
    withTransaction(context) { ctx => {
      // Adjust the pattern to match the new fileName format
      val datasetNamePattern: Regex = """/([^/]+)/.*""".r

      // Extract 'datasetName' using the pattern
      val datasetName = datasetNamePattern.findFirstMatchIn(fileName) match {
        case Some(matched) => matched.group(1) // Extract the first group which is 'datasetName'
        case None => throw new RuntimeException("The fileName format is not correct") // Default value or handle error
      }

      // Extract the file path
      val filePath = Paths.get(fileName.substring(fileName.indexOf(s"/$datasetName/") + s"/$datasetName/".length))
      val datasetsOfEnvironment = retrieveDatasetsAndVersions(ctx, uid, eid)

      // Initialize datasetFileDesc as None
      var datasetFileDesc: Option[DatasetFileDesc] = None

      // Iterate over datasetsOfEnvironment to find a match based on datasetName
      datasetsOfEnvironment.foreach { datasetAndVersion =>
        if (datasetAndVersion.dataset.getName == datasetName) {
          datasetFileDesc = Some(new DatasetFileDesc(
            filePath,
            Paths.get(datasetAndVersion.dataset.getStoragePath),
            datasetAndVersion.version.getVersionHash))
        }
      }

      // Check if datasetFileDesc is set, if not, throw an exception
      datasetFileDesc.getOrElse(throw new RuntimeException("Given file is not found in the environment"))
    }
    }
  }


  private def getEnvironmentByEid(ctx: DSLContext, eid: UInteger): Environment = {
    val environmentDao: EnvironmentDao = new EnvironmentDao(ctx.configuration())
    val env = environmentDao.fetchOneByEid(eid)

    if (env == null) {
      throw new Exception("Environment is not found")
    }

    env
  }
  private def doesUserOwnEnvironment(ctx: DSLContext, uid: UInteger, eid: UInteger): Boolean = {
    val environment = getEnvironmentByEid(ctx, eid)
    environment.getOwnerUid == uid
  }

  private def doesDatasetExistInEnvironment(
      ctx: DSLContext,
      did: UInteger,
      eid: UInteger
  ): Boolean = {
    val count = ctx
      .selectCount()
      .from(DATASET_OF_ENVIRONMENT)
      .where(
        DATASET_OF_ENVIRONMENT.EID
          .eq(eid)
          .and(DATASET_OF_ENVIRONMENT.DID.eq(did))
      )
      .fetchOne() // Fetch the record

    val countVal = count.getValue(0, classOf[Int]) // Get the count value from the record
    countVal > 0
  }

  private def fetchEnvironmentIdOfWorkflow(ctx: DSLContext, wid: UInteger): Option[UInteger] = {
    val environmentOfWorkflowDao = new EnvironmentOfWorkflowDao(ctx.configuration())
    val environmentOfWorkflow = environmentOfWorkflowDao.fetchByWid(wid)
    if (environmentOfWorkflow.isEmpty) {
      None
    } else {
      Some(environmentOfWorkflow.get(0).getEid)
    }
  }

  private def fetchWorkflowIdsOfEnvironment(ctx: DSLContext, eid: UInteger): List[UInteger] = {
    val environmentOfWorkflowDao = new EnvironmentOfWorkflowDao(ctx.configuration())
    val envOfWorkflows = environmentOfWorkflowDao.fetchByEid(eid)

    // Extract wids from envOfWorkflows and collect them into a list
    val workflowIds = envOfWorkflows.asScala.map(_.getWid).toList
    workflowIds
  }
  private def userHasWriteAccessToEnvironment(
      ctx: DSLContext,
      eid: UInteger,
      uid: UInteger
  ): Boolean = {
    // if user is the owner of the environment, return true
    if (doesUserOwnEnvironment(ctx, uid, eid)) {
      return true
    }

    // else, check the corresponding workflow if any, see if user has the write access to that workflow
    fetchWorkflowIdsOfEnvironment(ctx, eid).foreach(wid => {
      if (WorkflowAccessResource.hasWriteAccess(wid, uid)) {
        return true
      }
    })
    false
  }
  private def userHasReadAccessToEnvironment(
      ctx: DSLContext,
      eid: UInteger,
      uid: UInteger
  ): Boolean = {
    // if user is the owner of the environment, return true
    if (doesUserOwnEnvironment(ctx, uid, eid)) {
      return true
    }

    // else, check the corresponding workflow if any, see if user has the read access to that workflow
    fetchWorkflowIdsOfEnvironment(ctx, eid).foreach(wid => {
      if (WorkflowAccessResource.hasReadAccess(wid, uid)) {
        return true
      }
    })
    false
  }

  private def retrieveDatasetsAndVersions(
      ctx: DSLContext,
      uid: UInteger,
      eid: UInteger
  ): List[DatasetOfEnvironmentDetails] = {
    val datasetOfEnvironmentDao = new DatasetOfEnvironmentDao(ctx.configuration())
    val datasetsOfEnvironment = datasetOfEnvironmentDao.fetchByEid(eid).asScala

    datasetsOfEnvironment.map { datasetOfEnvironment =>
      val did = datasetOfEnvironment.getDid
      val dvid = datasetOfEnvironment.getDvid

      // Check for read access to the dataset
      if (!DatasetAccessResource.userHasReadAccess(ctx, did, uid)) {
        throw new Exception(UserNoPermissionExceptionMessage)
      }

      val datasetDao = new DatasetDao(ctx.configuration())
      val datasetVersionDao = new DatasetVersionDao(ctx.configuration())

      // Retrieve the Dataset and DatasetVersion
      val dataset = datasetDao.fetchOneByDid(did)
      val datasetVersion = datasetVersionDao.fetchOneByDvid(dvid)

      if (dataset == null || datasetVersion == null) {
        throw new Exception(EnvironmentNotFoundMessage) // Dataset or its version not found
      }

      DatasetOfEnvironmentDetails(dataset, datasetVersion)
    }.toList
  }

  private def retrieveDatasetsOfEnvironmentFileList(
      ctx: DSLContext,
      uid: UInteger,
      datasetsOfEnv: List[DatasetOfEnvironmentDetails]
  ): List[String] = {
    datasetsOfEnv.flatMap(entry => {
      val did = entry.dataset.getDid
      val dvid = entry.version.getDvid
      val datasetName = entry.dataset.getName
      val fileList = retrieveDatasetVersionFilePaths(ctx, uid, did, dvid)
      val resList: ListBuffer[String] = new ListBuffer[String]
      fileList.forEach(file => resList.append(s"/$datasetName/$file"))
      resList.toList
    })
  }

  case class DashboardEnvironment(
      environment: Environment,
      isCurrentUserEditable: Boolean
  )

  case class DatasetOfEnvironmentDetails(
      dataset: Dataset,
      version: DatasetVersion
  )

  case class EnvironmentIDs(eids: List[UInteger])

  case class DatasetID(did: UInteger)
  case class WorkflowLink(wid: UInteger)

  // error handling
  private val UserNoPermissionExceptionMessage = "user has no permission for the environment"
  private val EnvironmentNotFoundMessage = "environment not found"
  private val DatasetOfEnvironmentAlreadyExistsMessage =
    "the given dataset already exists in the environment"
  private val DatasetOfEnvironmentDoseNotExistMessage =
    "the given dataset does not exist in the environment"
}

@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/environment")
@Produces(Array(MediaType.APPLICATION_JSON))
class EnvironmentResource {

  @POST
  @Path("/create")
  def createEnvironment(@Auth user: SessionUser, environment: Environment): DashboardEnvironment = {

    withTransaction(context) { ctx =>
      {
        val uid = environment.getOwnerUid

        val createdEnvironment = EnvironmentResource.createEnvironment(
          ctx,
          uid,
          environment.getName,
          environment.getDescription
        )

        DashboardEnvironment(
          new Environment(
            createdEnvironment.getEid,
            createdEnvironment.getOwnerUid,
            createdEnvironment.getName,
            createdEnvironment.getDescription,
            createdEnvironment.getCreationTime
          ),
          isCurrentUserEditable = true
        )
      }
    }
  }

  @POST
  @Path("/delete")
  def deleteEnvironments(environmentIDs: EnvironmentIDs, @Auth user: SessionUser): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      val environmentDao: EnvironmentDao = new EnvironmentDao(ctx.configuration())

      for (eid <- environmentIDs.eids) {
        if (!doesUserOwnEnvironment(ctx, uid, eid)) {
          throw new Exception(UserNoPermissionExceptionMessage)
        }
        environmentDao.deleteById(eid)
      }

      Response.ok().build()
    }
  }

  @GET
  @Path("/{eid}")
  def retrieveEnvironmentByEid(
      @PathParam("eid") eid: UInteger,
      @Auth user: SessionUser
  ): DashboardEnvironment = {
    val uid = user.getUid

    withTransaction(context) { ctx =>
      if (!userHasReadAccessToEnvironment(ctx, eid, uid)) {
        throw new Exception(UserNoPermissionExceptionMessage)
      }
      val environment = getEnvironmentByEid(ctx, eid);
      DashboardEnvironment(
        environment = environment,
        isCurrentUserEditable = userHasWriteAccessToEnvironment(ctx, eid, uid)
      )
    }
  }

  @GET
  @Path("/{eid}/dataset")
  def getDatasetsOfEnvironment(
      @PathParam("eid") eid: UInteger,
      @Auth user: SessionUser
  ): List[DatasetOfEnvironment] = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasReadAccessToEnvironment(ctx, eid, uid)) {
        throw new Exception(UserNoPermissionExceptionMessage)
      }
      val datasetOfEnvironmentDao = new DatasetOfEnvironmentDao(ctx.configuration())
      val datasetsOfEnvironment = datasetOfEnvironmentDao.fetchByEid(eid)
      datasetsOfEnvironment.asScala.toList
    }
  }

  @GET
  @Path("/{eid}/dataset/details")
  def getDatasetsOfEnvironmentDetails(
      @PathParam("eid") eid: UInteger,
      @Auth user: SessionUser
  ): List[DatasetOfEnvironmentDetails] = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasReadAccessToEnvironment(ctx, eid, uid)) {
        throw new Exception(UserNoPermissionExceptionMessage)
      }
      retrieveDatasetsAndVersions(ctx, uid, eid)
    }
  }

  @POST
  @Path("/{eid}/dataset/add")
  def addDatasetForEnvironment(
      @PathParam("eid") eid: UInteger,
      @Auth user: SessionUser,
      datasetID: DatasetID
  ): Response = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      val did = datasetID.did

      if (
        !DatasetAccessResource.userHasReadAccess(ctx, did, uid)
        || !userHasWriteAccessToEnvironment(ctx, eid, uid)
      ) {
        return Response
          .status(Response.Status.FORBIDDEN)
          .entity(UserNoPermissionExceptionMessage)
          .build()
      }

      if (doesDatasetExistInEnvironment(ctx, did, eid)) {
        return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(DatasetOfEnvironmentAlreadyExistsMessage)
          .build()
      }

      val datasetOfEnvironmentDao = new DatasetOfEnvironmentDao(ctx.configuration())
      val latestDatasetVersion = DatasetResource.getDatasetLatestVersion(ctx, did, uid)
      datasetOfEnvironmentDao.insert(
        new DatasetOfEnvironment(
          did,
          eid,
          latestDatasetVersion.getDvid
        )
      )
      Response.status(Response.Status.OK).build()
    })
  }

  @POST
  @Path("/{eid}/dataset/remove")
  def removeDatasetForEnvironment(
      @PathParam("eid") eid: UInteger,
      @Auth user: SessionUser,
      datasetID: DatasetID
  ): Response = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      val did = datasetID.did

      if (
        !DatasetAccessResource.userHasReadAccess(ctx, did, uid)
        || !userHasWriteAccessToEnvironment(ctx, eid, uid)
      ) {
        return Response
          .status(Response.Status.FORBIDDEN)
          .entity(UserNoPermissionExceptionMessage)
          .build()
      }

      if (!doesDatasetExistInEnvironment(ctx, did, eid)) {
        return Response
          .status(Response.Status.BAD_REQUEST)
          .entity(DatasetOfEnvironmentDoseNotExistMessage)
          .build()
      }

      ctx
        .deleteFrom(DATASET_OF_ENVIRONMENT)
        .where(DATASET_OF_ENVIRONMENT.DID.eq(did))
        .and(DATASET_OF_ENVIRONMENT.EID.eq(eid))
        .execute()

      Response.status(Response.Status.OK).build()
    })
  }
  @POST
  @Path("/{eid}/linkWorkflow")
  def linkWorkflowToEnvironment(
      @PathParam("eid") eid: UInteger,
      @Auth user: SessionUser,
      workflowLink: WorkflowLink
  ): Response = {
    val uid = user.getUid

    withTransaction(context)(ctx => {
      val wid = workflowLink.wid
      if (!userHasReadAccessToEnvironment(ctx, eid, uid)) {
        return Response
          .status(Response.Status.FORBIDDEN)
          .entity(UserNoPermissionExceptionMessage)
          .build()
      }

      // Check if an entry with the specified wid already exists
      val exists = ctx
        .selectCount()
        .from(ENVIRONMENT_OF_WORKFLOW)
        .where(ENVIRONMENT_OF_WORKFLOW.WID.eq(wid))
        .fetchOne(0, classOf[Int]) > 0

      if (exists) {
        // Update the existing entry
        ctx
          .update(ENVIRONMENT_OF_WORKFLOW)
          .set(ENVIRONMENT_OF_WORKFLOW.EID, eid)
          .where(ENVIRONMENT_OF_WORKFLOW.WID.eq(wid))
          .execute()
      } else {
        // Insert a new entry
        ctx
          .insertInto(ENVIRONMENT_OF_WORKFLOW)
          .set(ENVIRONMENT_OF_WORKFLOW.EID, eid)
          .set(ENVIRONMENT_OF_WORKFLOW.WID, wid)
          .execute()
      }

      Response.ok().build()
    })
  }

  @GET
  @Path("/{eid}/files/{query:.*}")
  def getDatasetsFileList(
      @Auth user: SessionUser,
      @PathParam("eid") eid: UInteger,
      @PathParam("query") q: String
  ): List[String] = {
    val query = URLDecoder.decode(q, "UTF-8")
    val uid = user.getUid

    withTransaction(context)(ctx => {
      if (!userHasReadAccessToEnvironment(ctx, eid, uid)) {
        throw new Exception(UserNoPermissionExceptionMessage)
      }
      val datasetsOfEnv = retrieveDatasetsAndVersions(ctx, uid, eid)

      val result = ArrayBuffer[String]()
      val fileList = retrieveDatasetsOfEnvironmentFileList(ctx, uid, datasetsOfEnv)

      // Filter fileList based on query
      val filteredList = fileList.filter(filePath => query.isEmpty || filePath.contains(query))
      result ++= filteredList

      result.toList
    })
  }
}
