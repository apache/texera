package edu.uci.ics.texera.web.resource.dashboard.user.dataset

import edu.uci.ics.texera.Utils.withTransaction
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.enums.DatasetUserAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  DatasetDao,
  DatasetUserAccessDao,
  DatasetVersionDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{
  Dataset,
  DatasetUserAccess,
  DatasetVersion
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.web.model.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.SearchQueryParams
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetAccessResource.{
  getDatasetUserAccessPrivilege,
  userHasReadAccess,
  userHasWriteAccess,
  userOwnDataset
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.{
  DashboardDataset,
  DashboardDatasetVersion,
  DatasetDescriptionModification,
  DatasetIDs,
  DatasetNameModification,
  DatasetVersionRootFileNodes,
  DatasetVersions,
  ERR_DATASET_CREATION_FAILED_MESSAGE,
  ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE,
  context,
  createNewDatasetVersion,
  getDashboardDataset,
  getDatasetByID,
  getDatasetLatestVersion,
  getDatasetVersionHashByID
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.`type`.FileNode
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.service.GitVersionControlLocalFileStorage
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils.PathUtils
import io.dropwizard.auth.Auth
import org.glassfish.jersey.media.multipart.{FormDataMultiPart, FormDataParam}
import org.jooq.{DSLContext, EnumType}
import org.jooq.types.UInteger

import java.io.{InputStream, OutputStream}
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.security.RolesAllowed
import javax.ws.rs.{
  BadRequestException,
  Consumes,
  ForbiddenException,
  GET,
  NotFoundException,
  POST,
  Path,
  PathParam,
  Produces,
  QueryParam
}
import javax.ws.rs.core.{MediaType, Response, StreamingOutput}
import scala.jdk.CollectionConverters._

object DatasetResource {
  val DATASET_IS_PUBLIC: Byte = 1;

  val FILE_OPERATION_UPLOAD_PREFIX = "file:upload:"
  val FILE_OPERATION_REMOVE_PREFIX = "file:remove"

  val datasetLocks: scala.collection.concurrent.Map[UInteger, ReentrantLock] =
    new scala.collection.concurrent.TrieMap[UInteger, ReentrantLock]()

  private val context = SqlServer.createDSLContext()

  // error messages
  val ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE = "User has no read access to this dataset"
  val ERR_DATASET_NOT_FOUND_MESSAGE = "Dataset not found"
  val ERR_DATASET_VERSION_NOT_FOUND_MESSAGE = "The version of the dataset not found"
  val ERR_DATASET_CREATION_FAILED_MESSAGE =
    "Dataset creation is failed. Please make sure to upload files in order to create the initial version of dataset"

  // this function get the dataset from DB identified by did,
  // read access will be checked
  private def getDatasetByID(ctx: DSLContext, did: UInteger, uid: UInteger): Dataset = {
    if (!userHasReadAccess(ctx, did, uid)) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
    }
    val datasetDao = new DatasetDao(ctx.configuration())
    val dataset = datasetDao.fetchOneByDid(did)
    if (dataset == null) {
      throw new NotFoundException(ERR_DATASET_NOT_FOUND_MESSAGE)
    }
    dataset
  }

  // this function retrieve the version hash identified by dvid and did
  // read access will be checked
  private def getDatasetVersionHashByID(
      ctx: DSLContext,
      did: UInteger,
      dvid: UInteger,
      uid: UInteger
  ): String = {
    if (!userHasReadAccess(ctx, did, uid)) {
      throw new ForbiddenException("User has no read access to this dataset")
    }
    val datasetVersionDao = new DatasetVersionDao(ctx.configuration())
    val version = datasetVersionDao.fetchOneByDvid(dvid)
    if (version == null) {
      throw new NotFoundException("Dataset Version not found")
    }
    version.getVersionHash
  }

  // this function retrieve the DashboardDataset(Dataset from DB+more information) identified by did
  // read access will be checked
  def getDashboardDataset(ctx: DSLContext, did: UInteger, uid: UInteger): DashboardDataset = {
    if (!userHasReadAccess(ctx, did, uid)) {
      throw new ForbiddenException()
    }

    val targetDataset = getDatasetByID(ctx, did, uid)
    val userAccessPrivilege = getDatasetUserAccessPrivilege(ctx, did, uid)

    DashboardDataset(
      targetDataset,
      userAccessPrivilege,
      targetDataset.getOwnerUid == uid
    )
  }

  // this function retrieve the latest DatasetVersion from DB
  // the latest here means the one with latest creation time
  // read access will be checked
  def getDatasetLatestVersion(ctx: DSLContext, did: UInteger, uid: UInteger): DatasetVersion = {
    if (!userHasReadAccess(ctx, did, uid)) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
    }

    val latestVersion: DatasetVersion = ctx
      .selectFrom(DATASET_VERSION)
      .where(DATASET_VERSION.DID.eq(did))
      .orderBy(
        DATASET_VERSION.CREATION_TIME.desc()
      ) // Assuming latest version is the one with the most recent creation time
      .limit(1) // Limit to only one result
      .fetchOneInto(classOf[DatasetVersion])

    if (latestVersion == null) {
      throw new NotFoundException(ERR_DATASET_VERSION_NOT_FOUND_MESSAGE)
    }

    latestVersion
  }

  // this function create a new dataset version
  // the dataset is identified by did, the file changes/removals are contained in multiPart form
  // it returns the created dataset version if creation succeed, else return None
  // concurrency control is performed here: the thread has to have the lock in order to create the new version
  private def createNewDatasetVersion(
      ctx: DSLContext,
      did: UInteger,
      uid: UInteger,
      versionName: String,
      multiPart: FormDataMultiPart
  ): Option[DashboardDatasetVersion] = {

    // Acquire or Create the lock for dataset of {did}
    val lock = DatasetResource.datasetLocks.getOrElseUpdate(did, new ReentrantLock())

    if (lock.isLocked) {
      return None
    }
    lock.lock()
    try {
      val datasetPath = Paths.get(PathUtils.getDatasetPath(did).toString)

      // this is used to check if file operation happens
      var fileOperationHappens = false
      // for multipart, each file-related operation's key starts with file:
      // the operation is either upload or remove
      // for file:upload, the file path will be suffixed to it, e.g. file:upload:a/b/c.csv The value will be the file content
      // for file:remove, the value would be filepath1,filepath2
      val fields = multiPart.getFields().keySet().iterator()

      val commitHash = GitVersionControlLocalFileStorage.withCreateVersion(
        datasetPath,
        versionName,
        () => {
          while (fields.hasNext) {
            val fieldName = fields.next()
            val bodyPart = multiPart.getField(fieldName)

            if (fieldName.startsWith(FILE_OPERATION_UPLOAD_PREFIX)) {
              //        val contentDisposition = bodyPart.getContentDisposition
              //        val contentType = bodyPart.getMediaType.toString
              val filePath =
                datasetPath.resolve(fieldName.substring(FILE_OPERATION_UPLOAD_PREFIX.length))
              // TODO: be careful with the string operation here
              val value: InputStream = bodyPart.getValueAs(classOf[InputStream])
              GitVersionControlLocalFileStorage.writeFileToRepo(datasetPath, filePath, value)
              fileOperationHappens = true
            } else if (fieldName.startsWith(FILE_OPERATION_REMOVE_PREFIX)) {
              val filePathsValue = bodyPart.getValueAs(classOf[String])
              val filePaths = filePathsValue.split(",")
              filePaths.foreach { filePath =>
                val normalizedFilePath = filePath.stripPrefix("/")
                GitVersionControlLocalFileStorage.removeFileFromRepo(
                  datasetPath,
                  datasetPath.resolve(normalizedFilePath)
                )
              }
              fileOperationHappens = true
            }
          }
        }
      )

      if (!fileOperationHappens) {
        return None
      }

      // create the DatasetVersion that persists in the DB
      val datasetVersion = new DatasetVersion()

      datasetVersion.setName(versionName)
      datasetVersion.setDid(did)
      datasetVersion.setCreatorUid(uid)
      datasetVersion.setVersionHash(commitHash)

      Some(
        DashboardDatasetVersion(
          // insert the dataset version into DB, and fetch the newly-inserted one.
          ctx
            .insertInto(DATASET_VERSION) // Assuming DATASET is the table reference
            .set(ctx.newRecord(DATASET_VERSION, datasetVersion))
            .returning() // Assuming ID is the primary key column
            .fetchOne()
            .into(classOf[DatasetVersion]),
          GitVersionControlLocalFileStorage.retrieveRootFileNodesOfVersion(datasetPath, commitHash)
        )
      )
    } finally {
      // Release the lock
      lock.unlock()
    }
  }

  def retrieveDatasetVersionFilePaths(
      ctx: DSLContext,
      uid: UInteger,
      did: UInteger,
      dvid: UInteger
  ): util.List[String] = {
    val dataset = getDatasetByID(ctx, did, uid)
    val versionHash = getDatasetVersionHashByID(ctx, did, dvid, uid)

    val fileNodes = GitVersionControlLocalFileStorage.retrieveRootFileNodesOfVersion(
      Paths.get(dataset.getStoragePath),
      versionHash
    )

    FileNode.getAllFileRelativePaths(fileNodes)
  }

  case class DashboardDataset(
      dataset: Dataset,
      accessPrivilege: EnumType,
      isOwner: Boolean
  )

  case class DatasetVersionRootFileNodes(fileNodes: util.Set[FileNode])

  case class DatasetVersions(versions: List[DatasetVersion])

  case class DashboardDatasetVersion(
      datasetVersion: DatasetVersion,
      fileNodes: util.Set[FileNode]
  )

  case class DatasetIDs(dids: List[UInteger])

  case class DatasetNameModification(did: UInteger, name: String)

  case class DatasetDescriptionModification(did: UInteger, description: String)
}

@Produces(Array(MediaType.APPLICATION_JSON, "image/jpeg", "application/pdf"))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/dataset")
class DatasetResource {

  @POST
  @Path("/create")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def createDataset(
      @Auth user: SessionUser,
      @FormDataParam("datasetName") datasetName: String,
      @FormDataParam("datasetDescription") datasetDescription: String,
      @FormDataParam("isDatasetPublic") isDatasetPublic: String,
      @FormDataParam("initialVersionName") initialVersionName: String,
      files: FormDataMultiPart
  ): DashboardDataset = {

    withTransaction(context) { ctx =>
      val uid = user.getUid
      val datasetOfUserDao: DatasetUserAccessDao = new DatasetUserAccessDao(ctx.configuration())

      val dataset: Dataset = new Dataset()
      dataset.setName(datasetName)
      dataset.setDescription(datasetDescription)
      dataset.setIsPublic(isDatasetPublic.toByte)
      dataset.setOwnerUid(uid)

      val createdDataset = ctx
        .insertInto(DATASET)
        .set(ctx.newRecord(DATASET, dataset))
        .returning()
        .fetchOne()

      val did = createdDataset.getDid
      val datasetPath = PathUtils.getDatasetPath(did)
      createdDataset.setStoragePath(datasetPath.toString)
      createdDataset.update()

      val datasetUserAccess = new DatasetUserAccess()
      datasetUserAccess.setDid(createdDataset.getDid)
      datasetUserAccess.setUid(uid)
      datasetUserAccess.setPrivilege(DatasetUserAccessPrivilege.WRITE)
      datasetOfUserDao.insert(datasetUserAccess)

      // initialize the dataset directory
      GitVersionControlLocalFileStorage.initRepo(datasetPath)

      // create the initial version of the dataset
      val createdVersion = createNewDatasetVersion(ctx, did, uid, initialVersionName, files)

      createdVersion match {
        case Some(_) =>
        case None    =>
          // none means creation failed, user does not submit any files when creating the dataset
          throw new BadRequestException(ERR_DATASET_CREATION_FAILED_MESSAGE)
      }

      DashboardDataset(
        new Dataset(
          createdDataset.getDid,
          createdDataset.getOwnerUid,
          createdDataset.getName,
          createdDataset.getIsPublic,
          createdDataset.getStoragePath,
          createdDataset.getDescription,
          createdDataset.getCreationTime
        ),
        DatasetUserAccessPrivilege.WRITE,
        isOwner = true
      )
    }
  }

  @POST
  @Path("/delete")
  def deleteDataset(datasetIDs: DatasetIDs, @Auth user: SessionUser): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      for (did <- datasetIDs.dids) {
        if (!userOwnDataset(ctx, did, uid)) {
          // throw the exception that user has no access to certain dataset
          throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
        }
        // delete the dataset repo from the filesystem
        val dataset = getDatasetByID(ctx, did, uid)
        GitVersionControlLocalFileStorage.deleteRepo(Paths.get(dataset.getStoragePath))

        // delete the dataset from the DB
        datasetDao.deleteById(did)
      }

      Response.ok().build()
    }
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/update/name")
  def updateDatasetName(
      modificator: DatasetNameModification,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      val uid = sessionUser.getUid
      val did = modificator.did
      val name = modificator.name
      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val existedDataset = getDatasetByID(ctx, did, uid)
      existedDataset.setName(name)
      datasetDao.update(existedDataset)
      Response.ok().build()
    }
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/update/description")
  def updateDatasetDescription(
      modificator: DatasetDescriptionModification,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      val uid = sessionUser.getUid
      val did = modificator.did
      val description = modificator.description

      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val existedDataset = getDatasetByID(ctx, did, uid)
      existedDataset.setDescription(description)
      datasetDao.update(existedDataset)
      Response.ok().build()
    }
  }

  @POST
  @Path("/{did}/version/create")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def createDatasetVersion(
      @PathParam("did") did: UInteger,
      @FormDataParam("versionName") versionName: String,
      @Auth user: SessionUser,
      multiPart: FormDataMultiPart
  ): DashboardDatasetVersion = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      if (!userHasWriteAccess(ctx, did, uid)) {
        // throw the exception that user has no access to certain dataset
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      // create the version
      val createdVersion = createNewDatasetVersion(ctx, did, uid, versionName, multiPart)

      createdVersion match {
        case None =>
          throw new BadRequestException("User should do modifications to create a new version")
        case Some(version) => version
      }
    }
  }

  @GET
  @Path("/{did}")
  def getDataset(
      @PathParam("did") did: UInteger,
      @Auth user: SessionUser
  ): DashboardDataset = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      getDashboardDataset(ctx, did, uid)
    })
  }

  /**
    * This method returns a list of DashboardDatasets objects that are accessible by current user.
    * @param user the session user
    * @return list of user accessible DashboardDataset objects
    */
  @GET
  @Path("")
  def listDatasets(
      @Auth user: SessionUser
  ): List[DashboardDataset] = {
    val result = DashboardResource.searchAllResources(
      user,
      SearchQueryParams(resourceType = "dataset")
    )
    result.results.map(_.dataset.get)
  }

  @GET
  @Path("/{did}/version/list")
  def getDatasetVersionList(
      @PathParam("did") did: UInteger,
      @Auth user: SessionUser
  ): DatasetVersions = {
    val uid = user.getUid
    withTransaction(context)(ctx => {

      if (!userHasReadAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      val result: java.util.List[DatasetVersion] = ctx
        .selectFrom(DATASET_VERSION)
        .where(DATASET_VERSION.DID.eq(did))
        .orderBy(DATASET_VERSION.CREATION_TIME.desc()) // or .asc() for ascending
        .fetchInto(classOf[DatasetVersion])

      DatasetVersions(result.asScala.toList)
    })
  }

  @GET
  @Path("/{did}/version/latest")
  def getLatestDatasetVersion(
      @PathParam("did") did: UInteger,
      @Auth user: SessionUser
  ): DashboardDatasetVersion = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      val latestVersion = getDatasetLatestVersion(ctx, did, uid)
      val dataset = getDatasetByID(ctx, did, uid)
      val datasetPath = Paths.get(dataset.getStoragePath)

      DashboardDatasetVersion(
        latestVersion,
        GitVersionControlLocalFileStorage.retrieveRootFileNodesOfVersion(
          datasetPath,
          latestVersion.getVersionHash
        )
      )
    })
  }

  @GET
  @Path("/{did}/version/{dvid}/rootFileNodes")
  def retrieveDatasetVersionRootFileNodes(
      @PathParam("did") did: UInteger,
      @PathParam("dvid") dvid: UInteger,
      @Auth user: SessionUser
  ): DatasetVersionRootFileNodes = {
    val uid = user.getUid

    withTransaction(context)(ctx => {
      val targetDataset = getDatasetByID(ctx, did, uid)
      val targetDatasetPath = Paths.get(targetDataset.getStoragePath)
      val versionCommitHash = getDatasetVersionHashByID(ctx, did, dvid, uid)

      val fileTree = GitVersionControlLocalFileStorage.retrieveRootFileNodesOfVersion(
        targetDatasetPath,
        versionCommitHash
      )
      DatasetVersionRootFileNodes(fileTree)
    })
  }

  @GET
  @Path("/{did}/version/{dvid}/file")
  def retrieveDatasetSingleFile(
      @PathParam("did") did: UInteger,
      @PathParam("dvid") dvid: UInteger,
      @QueryParam("path") path: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      val decodedPath = URLDecoder.decode(path, StandardCharsets.UTF_8.name()).stripPrefix("/")

      val targetDataset = getDatasetByID(ctx, did, uid)
      val targetDatasetPath = Paths.get(targetDataset.getStoragePath)
      val versionCommitHash = getDatasetVersionHashByID(ctx, did, dvid, uid)

      val streamingOutput = new StreamingOutput() {
        override def write(output: OutputStream): Unit = {
          GitVersionControlLocalFileStorage.retrieveFileContentOfVersion(
            targetDatasetPath,
            versionCommitHash,
            targetDatasetPath.resolve(decodedPath),
            output
          )
        }
      }

      val contentType = decodedPath.split("\\.").lastOption.map(_.toLowerCase) match {
        case Some("jpg") | Some("jpeg") => "image/jpeg"
        case Some("png")                => "image/png"
        case Some("csv")                => "text/csv"
        case Some("md")                 => "text/markdown"
        case Some("txt")                => "text/plain"
        case Some("html") | Some("htm") => "text/html"
        case Some("json")               => "application/json"
        case Some("pdf")                => "application/pdf"
        case Some("doc") | Some("docx") => "application/msword"
        case Some("xls") | Some("xlsx") => "application/vnd.ms-excel"
        case Some("ppt") | Some("pptx") => "application/vnd.ms-powerpoint"
        case Some("mp4")                => "video/mp4"
        case Some("mp3")                => "audio/mpeg"
        case _                          => "application/octet-stream" // default binary format
      }

      Response.ok(streamingOutput).`type`(contentType).build()
    })
  }
}
