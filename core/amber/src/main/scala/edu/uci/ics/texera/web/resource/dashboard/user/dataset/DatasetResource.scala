package edu.uci.ics.texera.web.resource.dashboard.user.dataset

import edu.uci.ics.amber.engine.common.Utils.withTransaction
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
  DatasetVersion,
  User
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.web.model.jooq.generated.tables.User.USER
import edu.uci.ics.texera.web.model.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS
import edu.uci.ics.texera.web.model.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetAccessResource.{
  getDatasetUserAccessPrivilege,
  getOwner,
  userHasReadAccess,
  userHasWriteAccess,
  userOwnDataset
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.{
  DATASET_IS_PRIVATE,
  DATASET_IS_PUBLIC,
  DashboardDataset,
  DashboardDatasetVersion,
  DatasetDescriptionModification,
  DatasetIDs,
  DatasetNameModification,
  DatasetVersionRootFileNodes,
  DatasetVersionRootFileNodesResponse,
  DatasetVersions,
  ERR_DATASET_CREATION_FAILED_MESSAGE,
  ERR_DATASET_NAME_ALREADY_EXISTS,
  ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE,
  ListDatasetsResponse,
  calculateLatestDatasetVersionSize,
  calculateDatasetVersionSize,
  context,
  createNewDatasetVersionFromFormData,
  getDashboardDataset,
  getDatasetByID,
  getDatasetVersionByID,
  getDatasetVersions,
  getFileNodesOfCertainVersion,
  getLatestDatasetVersionWithAccessCheck,
  getUserDatasets,
  resolvePath,
  retrievePublicDatasets
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.`type`.{
  DatasetFileNode,
  PhysicalFileNode
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.service.GitVersionControlLocalFileStorage
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils.PathUtils
import io.dropwizard.auth.Auth
import org.apache.commons.lang3.StringUtils
import org.glassfish.jersey.media.multipart.{FormDataMultiPart, FormDataParam}
import org.jooq.{DSLContext, EnumType}
import org.jooq.types.UInteger
import play.api.libs.json.Json

import java.io.{IOException, InputStream, OutputStream}
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.zip.{ZipEntry, ZipOutputStream}
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
  QueryParam,
  WebApplicationException
}
import javax.ws.rs.core.{MediaType, Response, StreamingOutput}
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.Using
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object DatasetResource {
  val DATASET_IS_PUBLIC: Byte = 1;
  val DATASET_IS_PRIVATE: Byte = 0;
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
  val ERR_DATASET_NAME_ALREADY_EXISTS = "A dataset with the same name already exists."

  def sanitizePath(input: String): String = {
    // Define the characters you want to remove
    val sanitized = StringUtils.replaceEach(input, Array("/", "\\"), Array("", ""))
    sanitized
  }

  // this function get the dataset from DB identified by did,
  // read access will be checked
  private def getDatasetByID(ctx: DSLContext, did: UInteger): Dataset = {
    val datasetDao = new DatasetDao(ctx.configuration())
    val dataset = datasetDao.fetchOneByDid(did)
    if (dataset == null) {
      throw new NotFoundException(ERR_DATASET_NOT_FOUND_MESSAGE)
    }
    dataset
  }

  private def getDatasetByName(
      ctx: DSLContext,
      ownerEmail: String,
      datasetName: String
  ): Dataset = {
    ctx
      .select(DATASET.fields: _*)
      .from(DATASET)
      .leftJoin(USER)
      .on(USER.UID.eq(DATASET.OWNER_UID))
      .where(USER.EMAIL.eq(ownerEmail))
      .and(DATASET.NAME.eq(datasetName))
      .fetchOneInto(classOf[Dataset])
  }

  private def getDatasetVersionByName(
      ctx: DSLContext,
      did: UInteger,
      versionName: String
  ): DatasetVersion = {
    ctx
      .selectFrom(DATASET_VERSION)
      .where(DATASET_VERSION.DID.eq(did))
      .and(DATASET_VERSION.NAME.eq(versionName))
      .fetchOneInto(classOf[DatasetVersion])
  }

  // this function retrieve the version hash identified by dvid and did
  // read access will be checked
  private def getDatasetVersionByID(
      ctx: DSLContext,
      dvid: UInteger
  ): DatasetVersion = {
    val datasetVersionDao = new DatasetVersionDao(ctx.configuration())
    val version = datasetVersionDao.fetchOneByDvid(dvid)
    if (version == null) {
      throw new NotFoundException("Dataset Version not found")
    }
    version
  }

  // @param shouldContainFile a boolean flag indicating whether the path includes a fileRelativePath
  // when shouldContainFile is true, user given path is /ownerEmail/datasetName/versionName/fileRelativePath
  // e.g. /bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv
  //      ownerName is bob@texera.com; datasetName is twitterDataset, versionName is v1, fileRelativePath is california/irvine/tw1.csv
  // when shouldContainFile is false, user given path is /ownerEmail/datasetName/versionName
  // e.g. /bob@texera.com/twitterDataset/v1
  //      ownerName is bob@texera.com; datasetName is twitterDataset, versionName is v1
  def resolvePath(
      path: java.nio.file.Path,
      shouldContainFile: Boolean
  ): (String, Dataset, DatasetVersion, Option[java.nio.file.Path]) = {

    val pathSegments = (0 until path.getNameCount).map(path.getName(_).toString).toArray

    // The expected length of the path segments:
    // - If shouldContainFile is true, the path should include 4 segments: /ownerEmail/datasetName/versionName/fileRelativePath
    // - If shouldContainFile is false, the path should include only 3 segments: /ownerEmail/datasetName/versionName
    val expectedLength = if (shouldContainFile) 4 else 3

    if (pathSegments.length < expectedLength) {
      throw new BadRequestException(
        s"Invalid path format. Expected format: /ownerEmail/datasetName/versionName" +
          (if (shouldContainFile) "/fileRelativePath" else "")
      )
    }

    val ownerEmail = pathSegments(0)
    val datasetName = pathSegments(1)
    val versionName = pathSegments(2)

    val fileRelativePath =
      if (shouldContainFile) Some(Paths.get(pathSegments.drop(3).mkString("/"))) else None

    withTransaction(context) { ctx =>
      // Get the dataset by owner email and dataset name
      val dataset = getDatasetByName(ctx, ownerEmail, datasetName)
      if (dataset == null) {
        throw new NotFoundException("Dataset not found")
      }

      // Get the dataset version by dataset ID and version name
      val datasetVersion = getDatasetVersionByName(ctx, dataset.getDid, versionName)
      if (datasetVersion == null) {
        throw new NotFoundException("Dataset version not found")
      }

      (ownerEmail, dataset, datasetVersion, fileRelativePath)
    }
  }

  // this function retrieve the DashboardDataset(Dataset from DB+more information) identified by did
  // read access will be checked
  def getDashboardDataset(ctx: DSLContext, did: UInteger, uid: UInteger): DashboardDataset = {
    if (!userHasReadAccess(ctx, did, uid)) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
    }

    val targetDataset = getDatasetByID(ctx, did)
    val userAccessPrivilege = getDatasetUserAccessPrivilege(ctx, did, uid)

    DashboardDataset(
      targetDataset,
      getOwner(ctx, did).getEmail,
      userAccessPrivilege,
      targetDataset.getOwnerUid == uid,
      List(),
      calculateLatestDatasetVersionSize(did)
    )
  }

  // the format of dataset version name is: v{#n} - {user provided dataset version name}. e.g. v10 - new version
  private def generateDatasetVersionName(
      ctx: DSLContext,
      did: UInteger,
      userProvidedVersionName: String
  ): String = {
    val numberOfExistingVersions = ctx
      .selectFrom(DATASET_VERSION)
      .where(DATASET_VERSION.DID.eq(did))
      .fetch()
      .size()

    val sanitizedUserProvidedVersionName = sanitizePath(userProvidedVersionName)
    val res = if (sanitizedUserProvidedVersionName == "") {
      "v" + (numberOfExistingVersions + 1).toString
    } else {
      "v" + (numberOfExistingVersions + 1).toString + " - " + sanitizedUserProvidedVersionName
    }

    res
  }

  // this function retrieve the latest DatasetVersion from DB
  // the latest here means the one with latest creation time
  // read access will be checked
  private def fetchLatestDatasetVersionInternal(
      ctx: DSLContext,
      did: UInteger
  ): Option[DatasetVersion] = {
    ctx
      .selectFrom(DATASET_VERSION)
      .where(DATASET_VERSION.DID.eq(did))
      .orderBy(DATASET_VERSION.CREATION_TIME.desc())
      .limit(1)
      .fetchOptionalInto(classOf[DatasetVersion])
      .toScala
  }

  def getLatestDatasetVersionWithAccessCheck(
      ctx: DSLContext,
      did: UInteger,
      uid: UInteger
  ): DatasetVersion = {
    if (!userHasReadAccess(ctx, did, uid)) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
    }

    fetchLatestDatasetVersionInternal(ctx, did) match {
      case Some(latestVersion) => latestVersion
      case None                => throw new NotFoundException(ERR_DATASET_VERSION_NOT_FOUND_MESSAGE)
    }
  }

  def getDatasetFile(
      did: UInteger,
      dvid: UInteger,
      fileRelativePath: java.nio.file.Path
  ): InputStream = {
    val versionHash = getDatasetVersionByID(context, dvid).getVersionHash
    val datasetPath = PathUtils.getDatasetPath(did)
    GitVersionControlLocalFileStorage
      .retrieveFileContentOfVersionAsInputStream(
        PathUtils.getDatasetPath(did),
        versionHash,
        datasetPath.resolve(fileRelativePath)
      )
  }

  private def getFileNodesOfCertainVersion(
      ownerNode: DatasetFileNode,
      datasetName: String,
      ownerName: String
  ): List[DatasetFileNode] = {
    ownerNode.children.get
      .find(_.getName == datasetName)
      .head
      .children
      .get
      .find(_.getName == ownerName)
      .head
      .children
      .get
  }

  // DatasetOperation defines the operations that will be applied when creating a new dataset version
  private case class DatasetOperation(
      filesToAdd: Map[java.nio.file.Path, InputStream],
      filesToRemove: List[java.nio.file.Path]
  )

  private def parseUserUploadedFormToDatasetOperations(
      did: UInteger,
      multiPart: FormDataMultiPart
  ): DatasetOperation = {
    val datasetPath = PathUtils.getDatasetPath(did) // Obtain dataset base path

    // Mutable collections for constructing DatasetOperation
    val filesToAdd = mutable.Map[java.nio.file.Path, InputStream]()
    val filesToRemove = mutable.ListBuffer[java.nio.file.Path]()

    val fields = multiPart.getFields.keySet.iterator() // Get all field names

    // for multipart, each file-related operation's key starts with file:
    // the operation is either upload or remove
    // for file:upload, the file path will be suffixed to it, e.g. file:upload:a/b/c.csv The value will be the file content
    // for file:remove, the value would be filepath1,filepath2
    while (fields.hasNext) {
      val fieldName = fields.next()
      val bodyPart = multiPart.getField(fieldName) // Get the body part for the field

      if (fieldName.startsWith(FILE_OPERATION_UPLOAD_PREFIX)) {
        // Determine the relative file path and resolve it with the dataset base path
        val filePath = datasetPath.resolve(fieldName.substring(FILE_OPERATION_UPLOAD_PREFIX.length))
        val inputStream =
          bodyPart.getValueAs(classOf[InputStream]) // Get input stream from multipart
        filesToAdd.put(filePath, inputStream) // Add to the map for uploads
      } else if (fieldName.startsWith(FILE_OPERATION_REMOVE_PREFIX)) {
        val filePathsValue =
          bodyPart.getValueAs(classOf[String]) // Get the file paths as a comma-separated string
        Json
          .parse(filePathsValue)
          .as[List[String]]
          .foreach(pathStr => {
            val (_, _, _, fileRelativePath) =
              resolvePath(Paths.get(pathStr), shouldContainFile = true)

            fileRelativePath
              .map { path =>
                filesToRemove += datasetPath
                  .resolve(path) // When path exists, resolve it and add to filesToRemove
              }
              .getOrElse {
                throw new IllegalArgumentException("File relative path is missing")
              }
          })
      }
    }

    // Return a new DatasetOperation with the map and list
    DatasetOperation(filesToAdd.toMap, filesToRemove.toList)
  }

  // add file(s) to a dataset, a new version will be created
  def createNewDatasetVersionByAddingFiles(
      did: UInteger,
      user: User,
      filesToAdd: Map[java.nio.file.Path, InputStream]
  ): Option[DashboardDatasetVersion] = {
    applyDatasetOperationToCreateNewVersion(
      context,
      did,
      user.getUid,
      user.getEmail,
      "",
      DatasetOperation(filesToAdd, List())
    )
  }

  // create a new dataset version using the form data from frontend
  def createNewDatasetVersionFromFormData(
      ctx: DSLContext,
      did: UInteger,
      uid: UInteger,
      ownerEmail: String,
      userProvidedVersionName: String,
      multiPart: FormDataMultiPart
  ): Option[DashboardDatasetVersion] = {
    val datasetOperation = parseUserUploadedFormToDatasetOperations(did, multiPart)
    applyDatasetOperationToCreateNewVersion(
      ctx,
      did,
      uid,
      ownerEmail,
      userProvidedVersionName,
      datasetOperation
    )
  }

  // Private method to get user datasets
  private def getUserDatasets(ctx: DSLContext, uid: UInteger): List[Dataset] = {
    ctx
      .selectFrom(DATASET)
      .where(DATASET.OWNER_UID.eq(uid))
      .fetchInto(classOf[Dataset])
      .asScala
      .toList
  }

  private def getDatasetVersions(
      ctx: DSLContext,
      did: UInteger,
      uid: UInteger
  ): List[DatasetVersion] = {
    val result: java.util.List[DatasetVersion] = ctx
      .selectFrom(DATASET_VERSION)
      .where(DATASET_VERSION.DID.eq(did))
      .orderBy(DATASET_VERSION.CREATION_TIME.desc()) // or .asc() for ascending
      .fetchInto(classOf[DatasetVersion])

    result.toList
  }

  // apply the dataset operation to create a new dataset version
  // it returns the created dataset version if creation succeed, else return None
  // concurrency control is performed here: the thread has to have the lock in order to create the new version
  private def applyDatasetOperationToCreateNewVersion(
      ctx: DSLContext,
      did: UInteger,
      uid: UInteger,
      ownerEmail: String,
      userProvidedVersionName: String,
      datasetOperation: DatasetOperation
  ): Option[DashboardDatasetVersion] = {
    // Acquire or Create the lock for dataset of {did}
    val lock = DatasetResource.datasetLocks.getOrElseUpdate(did, new ReentrantLock())

    if (lock.isLocked) {
      return None
    }
    lock.lock()
    try {
      val dataset = getDatasetByID(ctx, did)
      val datasetPath = PathUtils.getDatasetPath(did)
      if (datasetOperation.filesToAdd.isEmpty && datasetOperation.filesToRemove.isEmpty) {
        return None
      }
      val datasetName = dataset.getName
      val versionName = generateDatasetVersionName(ctx, did, userProvidedVersionName)
      val commitHash = GitVersionControlLocalFileStorage.withCreateVersion(
        datasetPath,
        versionName,
        () => {
          datasetOperation.filesToAdd.foreach {
            case (filePath, fileStream) =>
              GitVersionControlLocalFileStorage.writeFileToRepo(datasetPath, filePath, fileStream)
          }

          datasetOperation.filesToRemove.foreach { filePath =>
            GitVersionControlLocalFileStorage.removeFileFromRepo(
              datasetPath,
              filePath
            )
          }
        }
      )

      // create the DatasetVersion that persists in the DB
      val datasetVersion = new DatasetVersion()

      datasetVersion.setName(versionName)
      datasetVersion.setDid(did)
      datasetVersion.setCreatorUid(uid)
      datasetVersion.setVersionHash(commitHash)

      val physicalFileNodes =
        GitVersionControlLocalFileStorage.retrieveRootFileNodesOfVersion(datasetPath, commitHash)
      Some(
        DashboardDatasetVersion(
          // insert the dataset version into DB, and fetch the newly-inserted one.
          ctx
            .insertInto(DATASET_VERSION) // Assuming DATASET is the table reference
            .set(ctx.newRecord(DATASET_VERSION, datasetVersion))
            .returning() // Assuming ID is the primary key column
            .fetchOne()
            .into(classOf[DatasetVersion]),
          DatasetFileNode.fromPhysicalFileNodes(
            Map(
              (ownerEmail, datasetName, versionName) -> physicalFileNodes.toList
            )
          )
        )
      )
    } finally {
      // Release the lock
      lock.unlock()
    }
  }

  private def retrievePublicDatasets(ctx: DSLContext): util.List[DashboardDataset] = {
    ctx
      .select()
      .from(
        DATASET
          .leftJoin(USER)
          .on(USER.UID.eq(DATASET.OWNER_UID))
      )
      .where(DATASET.IS_PUBLIC.eq(DATASET_IS_PUBLIC))
      .fetch()
      .map(record => {
        val dataset = record.into(DATASET).into(classOf[Dataset])
        val ownerEmail = record.into(USER).getEmail
        DashboardDataset(
          isOwner = false,
          dataset = dataset,
          accessPrivilege = DatasetUserAccessPrivilege.READ,
          versions = List(),
          ownerEmail = ownerEmail,
          size = calculateLatestDatasetVersionSize(dataset.getDid)
        )
      })
  }

  case class DashboardDataset(
      dataset: Dataset,
      ownerEmail: String,
      accessPrivilege: EnumType,
      isOwner: Boolean,
      versions: List[DashboardDatasetVersion],
      size: Long
  )

  case class ListDatasetsResponse(
      datasets: List[DashboardDataset],
      fileNodes: List[DatasetFileNode]
  )

  case class DatasetVersionRootFileNodes(fileNodes: List[DatasetFileNode])

  case class DatasetVersions(versions: List[DatasetVersion])

  case class DashboardDatasetVersion(
      datasetVersion: DatasetVersion,
      fileNodes: List[DatasetFileNode]
  )

  case class DatasetIDs(dids: List[UInteger])

  case class DatasetNameModification(did: UInteger, name: String)

  case class DatasetDescriptionModification(did: UInteger, description: String)

  /*
   If versionHash is provided, calculate the size of the specific version of the dataset.
   Otherwise, calculate the size of the latest version of the dataset.
   */
  private def calculateSize(did: UInteger, versionHash: Option[String] = None): Long = {
    Try {
      val datasetPath = PathUtils.getDatasetPath(did)
      val hash = versionHash.getOrElse {
        fetchLatestDatasetVersionInternal(context, did)
          .map(_.getVersionHash)
          .getOrElse(throw new NoSuchElementException("No versions found for this dataset"))
      }

      val fileNodes = GitVersionControlLocalFileStorage.retrieveRootFileNodesOfVersion(
        datasetPath,
        hash
      )

      calculateSizeFromPhysicalNodes(fileNodes)
    } match {
      case Success(size) => size
      case Failure(exception) =>
        val errorMessage = versionHash.map(_ => "dataset version").getOrElse("dataset")
        println(s"Error calculating $errorMessage size: ${exception.getMessage}")
        0L
    }
  }

  def calculateDatasetVersionSize(did: UInteger, dvid: UInteger): Long = {
    val versionHash = getDatasetVersionByID(context, dvid).getVersionHash
    calculateSize(did, Some(versionHash))
  }

  def calculateLatestDatasetVersionSize(did: UInteger): Long = {
    calculateSize(did)
  }

  private def calculateSizeFromPhysicalNodes(nodes: java.util.Set[PhysicalFileNode]): Long = {
    nodes.asScala.foldLeft(0L) { (totalSize, node) =>
      totalSize + (if (node.isDirectory) {
                     calculateSizeFromPhysicalNodes(node.getChildren)
                   } else {
                     node.getSize
                   })
    }
  }

  case class DatasetVersionRootFileNodesResponse(
      rootFileNodes: DatasetVersionRootFileNodes,
      size: Long
  )
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

      // do the name duplication check
      val existingDatasets = getUserDatasets(ctx, uid)
      if (existingDatasets.exists(_.getName == datasetName)) {
        throw new BadRequestException(ERR_DATASET_NAME_ALREADY_EXISTS)
      }

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

      val datasetUserAccess = new DatasetUserAccess()
      datasetUserAccess.setDid(createdDataset.getDid)
      datasetUserAccess.setUid(uid)
      datasetUserAccess.setPrivilege(DatasetUserAccessPrivilege.WRITE)
      datasetOfUserDao.insert(datasetUserAccess)

      // initialize the dataset directory
      GitVersionControlLocalFileStorage.initRepo(datasetPath)

      // create the initial version of the dataset
      val createdVersion =
        createNewDatasetVersionFromFormData(ctx, did, uid, user.getEmail, initialVersionName, files)

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
          createdDataset.getDescription,
          createdDataset.getCreationTime
        ),
        user.getEmail,
        DatasetUserAccessPrivilege.WRITE,
        isOwner = true,
        versions = List(),
        size = calculateLatestDatasetVersionSize(did)
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
        GitVersionControlLocalFileStorage.deleteRepo(PathUtils.getDatasetPath(did))

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

      val existedDataset = getDatasetByID(ctx, did)
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

      val existedDataset = getDatasetByID(ctx, did)
      existedDataset.setDescription(description)
      datasetDao.update(existedDataset)
      Response.ok().build()
    }
  }

  @POST
  @Path("/{did}/update/publicity")
  def toggleDatasetPublicity(
      @PathParam("did") did: UInteger,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      val uid = sessionUser.getUid

      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val existedDataset = getDatasetByID(ctx, did)
      if (existedDataset.getIsPublic == DATASET_IS_PUBLIC) {
        existedDataset.setIsPublic(DATASET_IS_PRIVATE)
      } else {
        existedDataset.setIsPublic(DATASET_IS_PUBLIC)
      }

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
      val createdVersion =
        createNewDatasetVersionFromFormData(ctx, did, uid, user.getEmail, versionName, multiPart)

      createdVersion match {
        case None =>
          throw new BadRequestException("User should do modifications to create a new version")
        case Some(version) => version
      }
    }
  }

  /**
    * This method returns a list of DashboardDatasets objects that are accessible by current user.
    * @param user the session user
    * @return list of user accessible DashboardDataset objects
    */
  @GET
  @Path("")
  def listDatasets(
      @Auth user: SessionUser,
      @QueryParam("includeVersions") includeVersions: Boolean = false,
      @QueryParam("includeFileNodes") includeFileNodes: Boolean = false,
      @QueryParam("path") filePathStr: String
  ): ListDatasetsResponse = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      var accessibleDatasets: ListBuffer[DashboardDataset] = ListBuffer()

      if (filePathStr != null && filePathStr.nonEmpty) {
        // if the file path is given, then only fetch the dataset and version this file is belonging to
        val decodedPathStr = URLDecoder.decode(filePathStr, StandardCharsets.UTF_8.name())
        val (ownerEmail, dataset, version, _) =
          resolvePath(Paths.get(decodedPathStr), shouldContainFile = true)
        val accessPrivilege = getDatasetUserAccessPrivilege(ctx, dataset.getDid, uid)
        if (
          accessPrivilege == DatasetUserAccessPrivilege.NONE && dataset.getIsPublic == DATASET_IS_PRIVATE
        ) {
          throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
        }
        accessibleDatasets = accessibleDatasets :+ DashboardDataset(
          dataset = dataset,
          ownerEmail = ownerEmail,
          accessPrivilege = accessPrivilege,
          isOwner = dataset.getOwnerUid == uid,
          versions = List(
            DashboardDatasetVersion(
              datasetVersion = version,
              fileNodes = List()
            )
          ),
          size = calculateLatestDatasetVersionSize(dataset.getDid)
        )
      } else {
        // first fetch all datasets user have explicit access to
        accessibleDatasets = ListBuffer.from(
          ctx
            .select()
            .from(
              DATASET
                .leftJoin(DATASET_USER_ACCESS)
                .on(DATASET_USER_ACCESS.DID.eq(DATASET.DID))
                .leftJoin(USER)
                .on(USER.UID.eq(DATASET.OWNER_UID))
            )
            .where(DATASET_USER_ACCESS.UID.eq(uid))
            .fetch()
            .map(record => {
              val dataset = record.into(DATASET).into(classOf[Dataset])
              val datasetAccess = record.into(DATASET_USER_ACCESS).into(classOf[DatasetUserAccess])
              val ownerEmail = record.into(USER).getEmail
              DashboardDataset(
                isOwner = dataset.getOwnerUid == uid,
                dataset = dataset,
                accessPrivilege = datasetAccess.getPrivilege,
                versions = List(),
                ownerEmail = ownerEmail,
                size = calculateLatestDatasetVersionSize(dataset.getDid)
              )
            })
        )

        // then we fetch the public datasets and merge it as a part of the result if not exist
        val publicDatasets = retrievePublicDatasets(context)
        publicDatasets.forEach { publicDataset =>
          if (!accessibleDatasets.exists(_.dataset.getDid == publicDataset.dataset.getDid)) {
            val dashboardDataset = DashboardDataset(
              isOwner = false,
              dataset = publicDataset.dataset,
              ownerEmail = publicDataset.ownerEmail,
              accessPrivilege = DatasetUserAccessPrivilege.READ,
              versions = List(),
              size = calculateLatestDatasetVersionSize(publicDataset.dataset.getDid)
            )
            accessibleDatasets = accessibleDatasets :+ dashboardDataset
          }
        }
      }
      val fileNodesMap = mutable.Map[(String, String, String), List[PhysicalFileNode]]()

      // iterate over datasets and retrieve the version
      accessibleDatasets = accessibleDatasets.map { dataset =>
        val did = dataset.dataset.getDid
        val size = DatasetResource.calculateLatestDatasetVersionSize(did)

        DashboardDataset(
          isOwner = dataset.isOwner,
          dataset = dataset.dataset,
          ownerEmail = dataset.ownerEmail,
          accessPrivilege = dataset.accessPrivilege,
          versions = if (includeVersions || includeFileNodes) {
            getDatasetVersions(ctx, did, uid).map { version =>
              if (includeFileNodes) {
                fileNodesMap += ((
                  dataset.ownerEmail,
                  dataset.dataset.getName,
                  version.getName
                ) -> GitVersionControlLocalFileStorage
                  .retrieveRootFileNodesOfVersion(
                    PathUtils.getDatasetPath(did),
                    version.getVersionHash
                  )
                  .toList)
              }
              DashboardDatasetVersion(
                version,
                List()
              )
            }
          } else {
            dataset.versions
          },
          size = size
        )
      }

      ListDatasetsResponse(
        accessibleDatasets.toList,
        DatasetFileNode.fromPhysicalFileNodes(fileNodesMap.toMap)
      )
    })
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
      if (!userHasReadAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      val dataset = getDatasetByID(ctx, did)
      val latestVersion = getLatestDatasetVersionWithAccessCheck(ctx, did, uid)
      val datasetPath = PathUtils.getDatasetPath(did)

      val ownerNode = DatasetFileNode
        .fromPhysicalFileNodes(
          Map(
            (user.getEmail, dataset.getName, latestVersion.getName) ->
              GitVersionControlLocalFileStorage
                .retrieveRootFileNodesOfVersion(
                  datasetPath,
                  latestVersion.getVersionHash
                )
                .toList
          )
        )
        .head

      DashboardDatasetVersion(
        latestVersion,
        getFileNodesOfCertainVersion(ownerNode, dataset.getName, latestVersion.getName)
      )
    })
  }

  @GET
  @Path("/{did}/version/{dvid}/rootFileNodes")
  def retrieveDatasetVersionRootFileNodes(
      @PathParam("did") did: UInteger,
      @PathParam("dvid") dvid: UInteger,
      @Auth user: SessionUser
  ): DatasetVersionRootFileNodesResponse = {
    val uid = user.getUid

    withTransaction(context)(ctx => {
      val dataset = getDashboardDataset(ctx, did, uid)
      val targetDatasetPath = PathUtils.getDatasetPath(did)
      val datasetVersion = getDatasetVersionByID(ctx, dvid)
      val datasetName = dataset.dataset.getName
      val fileNodes = GitVersionControlLocalFileStorage.retrieveRootFileNodesOfVersion(
        targetDatasetPath,
        datasetVersion.getVersionHash
      )
      val size = calculateDatasetVersionSize(did, dvid)
      val ownerFileNode = DatasetFileNode
        .fromPhysicalFileNodes(
          Map((dataset.ownerEmail, datasetName, datasetVersion.getName) -> fileNodes.toList)
        )
        .head

      DatasetVersionRootFileNodesResponse(
        DatasetVersionRootFileNodes(
          getFileNodesOfCertainVersion(ownerFileNode, datasetName, datasetVersion.getName)
        ),
        size
      )
    })
  }

  @GET
  @Path("/{did}")
  def getDataset(
      @PathParam("did") did: UInteger,
      @Auth user: SessionUser
  ): DashboardDataset = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      val dashboardDataset = getDashboardDataset(ctx, did, uid)
      val size = DatasetResource.calculateLatestDatasetVersionSize(did)
      dashboardDataset.copy(size = size)
    })
  }

  @GET
  @Path("/file")
  def retrieveDatasetSingleFile(
      @QueryParam("path") pathStr: String,
      @Auth user: SessionUser
  ): Response = {
    val uid = user.getUid
    val decodedPathStr = URLDecoder.decode(pathStr, StandardCharsets.UTF_8.name())

    val (_, dataset, dsVersion, fileRelativePath) =
      resolvePath(Paths.get(decodedPathStr), shouldContainFile = true)

    withTransaction(context)(ctx => {
      val did = dataset.getDid
      val dvid = dsVersion.getDvid

      if (!userHasReadAccess(ctx, dataset.getDid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val targetDatasetPath = PathUtils.getDatasetPath(did)
      val datasetVersion = getDatasetVersionByID(ctx, dvid)

      val streamingOutput = new StreamingOutput() {
        override def write(output: OutputStream): Unit = {
          fileRelativePath
            .map { path =>
              GitVersionControlLocalFileStorage.retrieveFileContentOfVersion(
                targetDatasetPath,
                datasetVersion.getVersionHash,
                targetDatasetPath.resolve(path),
                output
              )
            }
            .getOrElse {
              throw new IllegalArgumentException("File relative path is missing.")
            }
        }
      }

      val contentType = decodedPathStr.split("\\.").lastOption.map(_.toLowerCase) match {
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

  /**
    * Retrieves a ZIP file for a specific dataset version or the latest version.
    *
    * @param pathStr The dataset version path in the format: /ownerEmail/datasetName/versionName
    *                Example: /user@example.com/dataset/v1
    * @param getLatest When true, retrieves the latest version regardless of the provided path.
    * @param did The dataset ID (used when getLatest is true).
    * @param user The session user.
    * @return A Response containing the dataset version as a ZIP file.
    */
  @GET
  @Path("/version-zip")
  def retrieveDatasetVersionZip(
      @QueryParam("path") pathStr: String,
      @QueryParam("getLatest") getLatest: Boolean,
      @QueryParam("did") did: UInteger,
      @Auth user: SessionUser
  ): Response = {
    val (dataset, version) = if (getLatest) {
      getLatestVersionInfo(did, user)
    } else {
      resolveAndValidatePath(pathStr, user)
    }
    val targetDatasetPath = PathUtils.getDatasetPath(dataset.getDid)
    val fileNodes = GitVersionControlLocalFileStorage.retrieveRootFileNodesOfVersion(
      targetDatasetPath,
      version.getVersionHash
    )

    val streamingOutput = new StreamingOutput {
      override def write(outputStream: OutputStream): Unit = {
        Using(new ZipOutputStream(outputStream)) { zipOutputStream =>
          def addFileNodeToZip(fileNode: PhysicalFileNode): Unit = {
            val relativePath = fileNode.getRelativePath.toString

            if (fileNode.isDirectory) {
              // For directories, add a ZIP entry with a trailing slash
              zipOutputStream.putNextEntry(new ZipEntry(relativePath + "/"))
              zipOutputStream.closeEntry()

              // Recursively add children
              fileNode.getChildren.asScala.foreach(addFileNodeToZip)
            } else {
              // For files, add the file content
              try {
                zipOutputStream.putNextEntry(new ZipEntry(relativePath))
                Using(Files.newInputStream(fileNode.getAbsolutePath)) { inputStream =>
                  inputStream.transferTo(zipOutputStream)
                }
              } catch {
                case e: IOException =>
                  throw new WebApplicationException(s"Error processing file: $relativePath", e)
              } finally {
                zipOutputStream.closeEntry()
              }
            }
          }

          // Start the recursive process for each root file node
          fileNodes.asScala.foreach(addFileNodeToZip)
        }.recover {
          case e: IOException =>
            throw new WebApplicationException("Error creating ZIP output stream", e)
          case NonFatal(e) =>
            throw new WebApplicationException("Unexpected error while creating ZIP", e)
        }
      }
    }

    Response
      .ok(streamingOutput)
      .header(
        "Content-Disposition",
        s"attachment; filename=${dataset.getName}-${version.getName}.zip"
      )
      .`type`("application/zip")
      .build()
  }

  private def resolveAndValidatePath(
      pathStr: String,
      user: SessionUser
  ): (Dataset, DatasetVersion) = {
    val decodedPathStr = URLDecoder.decode(pathStr, StandardCharsets.UTF_8.name())
    val (_, dataset, dsVersion, _) =
      resolvePath(Paths.get(decodedPathStr), shouldContainFile = false)

    validateUserAccess(dataset.getDid, user.getUid)
    (dataset, dsVersion)
  }

  private def getLatestVersionInfo(did: UInteger, user: SessionUser): (Dataset, DatasetVersion) = {
    validateUserAccess(did, user.getUid)
    val dataset = getDatasetByID(context, did)
    val latestVersion = getLatestDatasetVersionWithAccessCheck(context, did, user.getUid)
    (dataset, latestVersion)
  }

  private def validateUserAccess(did: UInteger, uid: UInteger): Unit = {
    if (!userHasReadAccess(context, did, uid)) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
    }
  }
}
