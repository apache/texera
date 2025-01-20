package edu.uci.ics.texera.web.resource.dashboard.user.dataset

import edu.uci.ics.amber.core.storage.{DocumentFactory, FileResolver, StorageConfig}
import edu.uci.ics.amber.core.storage.util.dataset.{
  GitVersionControlLocalFileStorage,
  PhysicalFileNode
}
import edu.uci.ics.amber.engine.common.Utils.withTransaction
import edu.uci.ics.amber.util.PathUtils
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.dao.jooq.generated.enums.DatasetUserAccessPrivilege
import edu.uci.ics.texera.dao.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.dao.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS
import edu.uci.ics.texera.dao.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import edu.uci.ics.texera.dao.jooq.generated.tables.User.USER
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{
  DatasetDao,
  DatasetUserAccessDao,
  DatasetVersionDao
}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{
  Dataset,
  DatasetUserAccess,
  DatasetVersion,
  User
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetAccessResource._
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetResource.{context, _}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.`type`.DatasetFileNode
import io.dropwizard.auth.Auth
import org.apache.commons.lang3.StringUtils
import org.glassfish.jersey.media.multipart.{FormDataMultiPart, FormDataParam}
import org.jooq.types.UInteger
import org.jooq.{DSLContext, EnumType}
import play.api.libs.json.Json

import java.io.{IOException, InputStream, OutputStream}
import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Optional
import java.util.concurrent.locks.ReentrantLock
import java.util.zip.{ZipEntry, ZipOutputStream}
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.{MediaType, Response, StreamingOutput}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try, Using}

object DatasetResource {
  private val DATASET_IS_PUBLIC: Byte = 1
  private val DATASET_IS_PRIVATE: Byte = 0
  private val FILE_OPERATION_UPLOAD_PREFIX = "file:upload:"
  private val FILE_OPERATION_REMOVE_PREFIX = "file:remove"

  private val datasetLocks: scala.collection.concurrent.Map[UInteger, ReentrantLock] =
    new scala.collection.concurrent.TrieMap[UInteger, ReentrantLock]()

  private val context = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
    .createDSLContext()

  /**
    * fetch the size of a certain dataset version.
    * @param did the target dataset id
    * @param versionHash the hash of the version. If None, fetch the latest version
    * @return
    */
  def calculateDatasetVersionSize(did: UInteger, versionHash: Option[String] = None): Long = {

    /**
      * Internal util to calculate the size from the physical nodes
      */
    def calculateSizeFromPhysicalNodes(nodes: java.util.Set[PhysicalFileNode]): Long = {
      nodes.asScala.foldLeft(0L) { (totalSize, node) =>
        totalSize + (if (node.isDirectory) {
                       calculateSizeFromPhysicalNodes(node.getChildren)
                     } else {
                       node.getSize
                     })
      }
    }

    Try {
      val datasetPath = PathUtils.getDatasetPath(did)
      val hash = versionHash.getOrElse {
        getLatestDatasetVersion(context, did)
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

  /**
    * Helper function to get the dataset from DB using did
    */
  private def getDatasetByID(ctx: DSLContext, did: UInteger): Dataset = {
    val datasetDao = new DatasetDao(ctx.configuration())
    val dataset = datasetDao.fetchOneByDid(did)
    if (dataset == null) {
      throw new NotFoundException(f"Dataset $did not found")
    }
    dataset
  }

  /**
    * Helper function to get the dataset version from DB using dvid
    */
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

  /**
    * Helper function to get the latest dataset version from the DB
    */
  private def getLatestDatasetVersion(
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

  // DatasetOperation defines the operations that will be applied when creating a new dataset version
  private case class DatasetOperation(
      filesToAdd: Map[java.nio.file.Path, InputStream],
      filesToRemove: List[URI]
  )

  private def parseUserUploadedFormToDatasetOperations(
      did: UInteger,
      multiPart: FormDataMultiPart
  ): DatasetOperation = {
    val datasetPath = PathUtils.getDatasetPath(did) // Obtain dataset base path

    // Mutable collections for constructing DatasetOperation
    val filesToAdd = mutable.Map[java.nio.file.Path, InputStream]()
    val filesToRemove = mutable.ListBuffer[URI]()

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
            filesToRemove += FileResolver.resolve(pathStr)
          })
      }
    }

    // Return a new DatasetOperation with the map and list
    DatasetOperation(filesToAdd.toMap, filesToRemove.toList)
  }

  /**
    * Create a new dataset version by adding new files
    * @param did the target dataset id
    * @param user the user submitting the request
    * @param filesToAdd the map containing the files to add
    * @return the created dataset version
    */
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
    // Helper function to generate the dataset version name
    // the format of dataset version name is: v{#n} - {user provided dataset version name}. e.g. v10 - new version
    def generateDatasetVersionName(
        ctx: DSLContext,
        did: UInteger,
        userProvidedVersionName: String
    ): String = {
      val numberOfExistingVersions = ctx
        .selectFrom(DATASET_VERSION)
        .where(DATASET_VERSION.DID.eq(did))
        .fetch()
        .size()

      val sanitizedUserProvidedVersionName =
        StringUtils.replaceEach(userProvidedVersionName, Array("/", "\\"), Array("", ""))
      val res = if (sanitizedUserProvidedVersionName == "") {
        "v" + (numberOfExistingVersions + 1).toString
      } else {
        "v" + (numberOfExistingVersions + 1).toString + " - " + sanitizedUserProvidedVersionName
      }

      res
    }

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

          datasetOperation.filesToRemove.foreach { fileUri =>
            DocumentFactory.openDocument(fileUri)._1.clear()
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
              (ownerEmail, datasetName, versionName) -> physicalFileNodes.asScala.toList
            )
          )
        )
      )
    } finally {
      // Release the lock
      lock.unlock()
    }
  }

  case class DashboardDataset(
      dataset: Dataset,
      ownerEmail: String,
      accessPrivilege: EnumType,
      isOwner: Boolean,
      versions: List[DashboardDatasetVersion],
      size: Long
  )
  case class DashboardDatasetVersion(
      datasetVersion: DatasetVersion,
      fileNodes: List[DatasetFileNode]
  )

  case class DatasetIDs(dids: List[UInteger])

  case class DatasetNameModification(did: UInteger, name: String)

  case class DatasetDescriptionModification(did: UInteger, description: String)

  case class DatasetVersionRootFileNodesResponse(
      fileNodes: List[DatasetFileNode],
      size: Long
  )
}

@Produces(Array(MediaType.APPLICATION_JSON, "image/jpeg", "application/pdf"))
@RolesAllowed(Array("REGULAR", "ADMIN"))
@Path("/dataset")
class DatasetResource {
  private val ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE = "User has no read access to this dataset"
  private val ERR_DATASET_VERSION_NOT_FOUND_MESSAGE = "The version of the dataset not found"
  private val ERR_DATASET_CREATION_FAILED_MESSAGE =
    "Dataset creation is failed. Please make sure to upload files in order to create the initial version of dataset"

  /**
    * Helper function to get the dataset from DB with additional information including user access privilege and owner email
    */
  private def getDashboardDataset(
      ctx: DSLContext,
      did: UInteger,
      uid: UInteger
  ): DashboardDataset = {
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
      calculateDatasetVersionSize(did)
    )
  }

  /**
    * Helper function to create a new dataset version using the given multi-part form.
    */
  private def createNewDatasetVersionFromFormData(
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
      val datasetDao: DatasetDao = new DatasetDao(ctx.configuration())
      val datasetOfUserDao: DatasetUserAccessDao = new DatasetUserAccessDao(ctx.configuration())

      // do the name duplication check
      val userExistingDatasetNames = datasetDao.fetchByOwnerUid(uid).asScala.map(_.getName)
      if (userExistingDatasetNames.contains(datasetName)) {
        throw new BadRequestException("Dataset with the same name already exists")
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
        size = calculateDatasetVersionSize(did)
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
    *
    * @param user the session user
    * @return list of user accessible DashboardDataset objects
    */
  @GET
  @Path("")
  def listDatasets(
      @Auth user: SessionUser
  ): List[DashboardDataset] = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      var accessibleDatasets: ListBuffer[DashboardDataset] = ListBuffer()
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
              size = calculateDatasetVersionSize(dataset.getDid)
            )
          })
          .asScala
      )

      // then we fetch the public datasets and merge it as a part of the result if not exist
      val publicDatasets = ctx
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
            size = calculateDatasetVersionSize(dataset.getDid)
          )
        })
      publicDatasets.forEach { publicDataset =>
        if (!accessibleDatasets.exists(_.dataset.getDid == publicDataset.dataset.getDid)) {
          val dashboardDataset = DashboardDataset(
            isOwner = false,
            dataset = publicDataset.dataset,
            ownerEmail = publicDataset.ownerEmail,
            accessPrivilege = DatasetUserAccessPrivilege.READ,
            versions = List(),
            size = calculateDatasetVersionSize(publicDataset.dataset.getDid)
          )
          accessibleDatasets = accessibleDatasets :+ dashboardDataset
        }
      }

      accessibleDatasets.toList
    })
  }

  @GET
  @Path("/{did}/version/list")
  def getDatasetVersionList(
      @PathParam("did") did: UInteger,
      @Auth user: SessionUser
  ): List[DatasetVersion] = {
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

      result.asScala.toList
    })
  }

  @GET
  @Path("/{did}/version/latest")
  def retrieveLatestDatasetVersion(
      @PathParam("did") did: UInteger,
      @Auth user: SessionUser
  ): DashboardDatasetVersion = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      if (!userHasReadAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      val dataset = getDatasetByID(ctx, did)
      val latestVersion = getLatestDatasetVersion(ctx, did).getOrElse(
        throw new NotFoundException(ERR_DATASET_VERSION_NOT_FOUND_MESSAGE)
      )
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
                .asScala
                .toList
          )
        )
        .head

      DashboardDatasetVersion(
        latestVersion,
        ownerNode.children.get
          .find(_.getName == dataset.getName)
          .head
          .children
          .get
          .find(_.getName == latestVersion.getName)
          .head
          .children
          .get
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
      val versionHash = getDatasetVersionByID(ctx, dvid).getVersionHash
      val size = calculateDatasetVersionSize(did, Some(versionHash))
      val ownerFileNode = DatasetFileNode
        .fromPhysicalFileNodes(
          Map((dataset.ownerEmail, datasetName, datasetVersion.getName) -> fileNodes.asScala.toList)
        )
        .head

      DatasetVersionRootFileNodesResponse(
        ownerFileNode.children.get
          .find(_.getName == datasetName)
          .head
          .children
          .get
          .find(_.getName == datasetVersion.getName)
          .head
          .children
          .get,
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
      val size = calculateDatasetVersionSize(did)
      dashboardDataset.copy(size = size)
    })
  }

  @GET
  @Path("/file")
  def retrieveDatasetSingleFile(
      @QueryParam("path") pathStr: String,
      @Auth user: SessionUser
  ): Response = {
    val decodedPathStr = URLDecoder.decode(pathStr, StandardCharsets.UTF_8.name())

    withTransaction(context)(ctx => {
      val fileUri = FileResolver.resolve(decodedPathStr)
      val streamingOutput = new StreamingOutput() {
        override def write(output: OutputStream): Unit = {
          val inputStream = DocumentFactory.openReadonlyDocument(fileUri).asInputStream()
          try {
            val buffer = new Array[Byte](8192) // buffer size
            var bytesRead = inputStream.read(buffer)
            while (bytesRead != -1) {
              output.write(buffer, 0, bytesRead)
              bytesRead = inputStream.read(buffer)
            }
          } finally {
            inputStream.close()
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
    * @param did  The dataset ID (used when getLatest is true).
    * @param dvid The dataset version ID, if given, retrieve this version; if not given, retrieve the latest version
    * @param user The session user.
    * @return A Response containing the dataset version as a ZIP file.
    */
  @GET
  @Path("/version-zip")
  def retrieveDatasetVersionZip(
      @QueryParam("did") did: UInteger,
      @QueryParam("dvid") dvid: Optional[Integer],
      @Auth user: SessionUser
  ): Response = {
    if (!userHasReadAccess(context, did, user.getUid)) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
    }
    val dataset = getDatasetByID(context, did)
    val version = if (dvid.isEmpty) {
      getLatestDatasetVersion(context, did).getOrElse(
        throw new NotFoundException(ERR_DATASET_VERSION_NOT_FOUND_MESSAGE)
      )
    } else {
      getDatasetVersionByID(context, UInteger.valueOf(dvid.get))
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
}
