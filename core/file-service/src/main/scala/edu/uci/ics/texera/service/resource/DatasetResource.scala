package edu.uci.ics.texera.service.resource

import edu.uci.ics.amber.core.storage.{
  DocumentFactory,
  FileResolver,
  LakeFSFileStorage,
  StorageConfig
}
import edu.uci.ics.amber.core.storage.util.dataset.{
  GitVersionControlLocalFileStorage,
  PhysicalFileNode
}
import edu.uci.ics.amber.util.PathUtils
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.SqlServer.withTransaction
import edu.uci.ics.texera.dao.jooq.generated.enums.PrivilegeEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.User.USER
import edu.uci.ics.texera.dao.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.dao.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS
import edu.uci.ics.texera.dao.jooq.generated.tables.DatasetVersion.DATASET_VERSION
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
import edu.uci.ics.texera.service.`type`.DatasetFileNode
import edu.uci.ics.texera.service.auth.SessionUser
import edu.uci.ics.texera.service.resource.DatasetAccessResource.{
  getDatasetUserAccessPrivilege,
  getOwner,
  isDatasetPublic,
  userHasReadAccess,
  userHasWriteAccess,
  userOwnDataset
}
import edu.uci.ics.texera.service.resource.DatasetResource.{
  DashboardDataset,
  DashboardDatasetVersion,
  DatasetDescriptionModification,
  DatasetVersionRootFileNodesResponse,
  calculateDatasetVersionSize,
  context,
  getDatasetByID,
  getDatasetVersionByID,
  getLatestDatasetVersion
}
import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs._
import jakarta.ws.rs.core.{MediaType, Response, StreamingOutput}
import org.apache.commons.lang3.StringUtils
import org.glassfish.jersey.media.multipart.FormDataParam
import org.jooq.{DSLContext, EnumType}

import java.io.{IOException, InputStream, OutputStream}
import java.net.{URI, URLDecoder}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Optional
import java.util.concurrent.locks.ReentrantLock
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try, Using}

object DatasetResource {
  private val context = SqlServer
    .getInstance()
    .createDSLContext()

  /**
    * Fetch the size of a certain dataset version.
    *
    * @param name         The target dataset's name (LakeFS repository name).
    * @param versionHash  The hash of the version. If None, fetch the latest version.
    * @return The total size of all objects in the dataset version.
    * @throws NoSuchElementException If the version hash is not found in the repository.
    */
  def calculateDatasetVersionSize(name: String, versionHash: Option[String] = None): Long = {
    // Retrieve all commits (versions) of the dataset repository
    val commits = LakeFSFileStorage.retrieveVersionsOfRepository(name)

    // Determine the target commit
    val targetCommit = versionHash match {
      case Some(hash) =>
        commits
          .find(_.getId == hash)
          .getOrElse(
            throw new NoSuchElementException(
              s"Version hash '$hash' not found in repository '$name'"
            )
          )
      case None =>
        commits.headOption // The latest commit (commits are sorted from latest to earliest)
          .getOrElse(throw new NoSuchElementException(s"No versions found for dataset '$name'"))
    }

    // Retrieve objects of the target version and sum up their sizes
    val objects = LakeFSFileStorage.retrieveObjectsOfVersion(name, targetCommit.getId)

    // Sum the sizes of all objects in the dataset version
    objects.map(_.getSizeBytes.longValue()).sum
  }

  /**
    * Helper function to get the dataset from DB using did
    */
  private def getDatasetByID(ctx: DSLContext, did: Integer): Dataset = {
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
      dvid: Integer
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
      did: Integer
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

  /**
    * Create a new dataset version by adding new files
    * @param did the target dataset id
    * @param user the user submitting the request
    * @param filesToAdd the map containing the files to add
    * @return the created dataset version
    */
  def createNewDatasetVersionByAddingFiles(
      did: Integer,
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
      did: Integer,
      uid: Integer,
      ownerEmail: String,
      userProvidedVersionName: String,
      datasetOperation: DatasetOperation
  ): Option[DashboardDatasetVersion] = {
    // Helper function to generate the dataset version name
    // the format of dataset version name is: v{#n} - {user provided dataset version name}. e.g. v10 - new version
    def generateDatasetVersionName(
        ctx: DSLContext,
        did: Integer,
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
  }

  case class DashboardDataset(
      dataset: Dataset,
      ownerEmail: String,
      accessPrivilege: EnumType,
      isOwner: Boolean
  )
  case class DashboardDatasetVersion(
      datasetVersion: DatasetVersion,
      fileNodes: List[DatasetFileNode]
  )

  case class DatasetIDs(dids: List[Integer])

  case class DatasetDescriptionModification(name: String, description: String)

  case class DatasetVersionRootFileNodesResponse(
      fileNodes: List[DatasetFileNode],
      size: Long
  )
}

@Produces(Array(MediaType.APPLICATION_JSON, "image/jpeg", "application/pdf"))
@Path("/dataset")
class DatasetResource {
  private val ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE = "User has no access to this dataset"
  private val ERR_DATASET_VERSION_NOT_FOUND_MESSAGE = "The version of the dataset not found"
  private val ERR_DATASET_CREATION_FAILED_MESSAGE =
    "Dataset creation is failed. Please make sure to upload files in order to create the initial version of dataset"

  /**
    * Helper function to get the dataset from DB with additional information including user access privilege and owner email
    */
  private def getDashboardDataset(
      ctx: DSLContext,
      did: Integer,
      uid: Option[Integer],
      isPublic: Boolean = false
  ): DashboardDataset = {
    if (
      (isPublic && !isDatasetPublic(ctx, did)) ||
      (!isPublic && (!userHasReadAccess(ctx, did, uid.get)))
    ) {
      throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
    }

    val targetDataset = getDatasetByID(ctx, did)
    val userAccessPrivilege =
      if (isPublic) PrivilegeEnum.NONE
      else getDatasetUserAccessPrivilege(ctx, did, uid.get)
    val isOwner = !isPublic && (targetDataset.getOwnerUid == uid.get)

    DashboardDataset(
      targetDataset,
      getOwner(ctx, did).getEmail,
      userAccessPrivilege,
      isOwner
    )
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/create")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def createDataset(
      @Auth user: SessionUser,
      @FormDataParam("datasetName") datasetName: String,
      @FormDataParam("datasetDescription") datasetDescription: String,
      @FormDataParam("isDatasetPublic") isDatasetPublic: String
  ): DashboardDataset = {

    withTransaction(context) { ctx =>
      val uid = user.getUid
      val datasetDao: DatasetDao = new DatasetDao(ctx.configuration())
      val datasetOfUserDao: DatasetUserAccessDao = new DatasetUserAccessDao(ctx.configuration())

      // do the name duplication check
      if (!datasetDao.fetchByName(datasetName).isEmpty) {
        throw new BadRequestException("Dataset with the same name already exists")
      }

      // Try to initialize the repository in LakeFS
      try {
        LakeFSFileStorage.initRepo(datasetName)
      } catch {
        case e: Exception =>
          throw new WebApplicationException(
            s"Failed to initialize repository in LakeFS: ${e.getMessage}"
          )
      }

      // insert the dataset into database
      val dataset: Dataset = new Dataset()
      dataset.setName(datasetName)
      dataset.setDescription(datasetDescription)
      dataset.setIsPublic(isDatasetPublic.toBoolean)
      dataset.setOwnerUid(uid)

      val createdDataset = ctx
        .insertInto(DATASET)
        .set(ctx.newRecord(DATASET, dataset))
        .returning()
        .fetchOne()

      // insert requester as the write access of the dataset
      val datasetUserAccess = new DatasetUserAccess()
      datasetUserAccess.setDid(createdDataset.getDid)
      datasetUserAccess.setUid(uid)
      datasetUserAccess.setPrivilege(PrivilegeEnum.WRITE)
      datasetOfUserDao.insert(datasetUserAccess)

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
        PrivilegeEnum.WRITE,
        isOwner = true
      )
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/delete")
  def deleteDataset(datasetName: String, @Auth user: SessionUser): Response = {
    val uid = user.getUid
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      val dataset = datasetDao.fetchByName(datasetName).asScala.toList
      if (dataset.isEmpty || !userOwnDataset(ctx, dataset.head.getDid, uid)) {
        // throw the exception that user has no access to certain dataset
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      try {
        LakeFSFileStorage.deleteRepo(datasetName)
      } catch {
        case e: Exception =>
          throw new WebApplicationException(
            s"Failed to delete a repository in LakeFS: ${e.getMessage}",
            e
          )
      }

      // delete the dataset from the DB
      datasetDao.deleteById(dataset.head.getDid)

      Response.ok().build()
    }
  }

  @POST
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/update/description")
  def updateDatasetDescription(
      modificator: DatasetDescriptionModification,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val uid = sessionUser.getUid

      val datasetDao = new DatasetDao(ctx.configuration())
      val datasets = datasetDao.fetchByName(modificator.name).asScala.toList
      if (datasets.isEmpty || !userHasWriteAccess(ctx, datasets.head.getDid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val datasetToChange = datasets.head
      datasetToChange.setDescription(modificator.description)
      datasetDao.update(datasetToChange)
      Response.ok().build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/update/publicity")
  def toggleDatasetPublicity(
      @PathParam("did") did: Integer,
      @Auth sessionUser: SessionUser
  ): Response = {
    withTransaction(context) { ctx =>
      val datasetDao = new DatasetDao(ctx.configuration())
      val uid = sessionUser.getUid

      if (!userHasWriteAccess(ctx, did, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }

      val existedDataset = getDatasetByID(ctx, did)
      existedDataset.setIsPublic(!existedDataset.getIsPublic)

      datasetDao.update(existedDataset)
      Response.ok().build()
    }
  }

  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{name}/version/create")
  @Consumes(Array(MediaType.MULTIPART_FORM_DATA))
  def createDatasetVersion(
      @PathParam("name") name: String,
      @FormDataParam("versionName") versionName: String,
      @Auth user: SessionUser
  ): Unit = {
    val uid = user.getUid
    // TODO: finish it
  }

  /**
    * This method returns a list of DashboardDatasets objects that are accessible by current user.
    *
    * @param user the session user
    * @return list of user accessible DashboardDataset objects
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/list")
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
              ownerEmail = ownerEmail
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
        .where(DATASET.IS_PUBLIC.eq(true))
        .fetch()
        .map(record => {
          val dataset = record.into(DATASET).into(classOf[Dataset])
          val ownerEmail = record.into(USER).getEmail
          DashboardDataset(
            isOwner = false,
            dataset = dataset,
            accessPrivilege = PrivilegeEnum.READ,
            ownerEmail = ownerEmail
          )
        })
      publicDatasets.forEach { publicDataset =>
        if (!accessibleDatasets.exists(_.dataset.getDid == publicDataset.dataset.getDid)) {
          val dashboardDataset = DashboardDataset(
            isOwner = false,
            dataset = publicDataset.dataset,
            ownerEmail = publicDataset.ownerEmail,
            accessPrivilege = PrivilegeEnum.READ
          )
          accessibleDatasets = accessibleDatasets :+ dashboardDataset
        }
      }

      accessibleDatasets.toList
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{name}/version/list")
  def getDatasetVersionList(
      @PathParam("name") name: String,
      @Auth user: SessionUser
  ): List[DatasetVersion] = {
    val uid = user.getUid
    withTransaction(context)(ctx => {
      val datasetDao = new DatasetDao(ctx.configuration())
      val datasets = datasetDao.fetchByName(name).asScala
      if (datasets.isEmpty || !userHasReadAccess(ctx, datasets.head.getDid, uid)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      fetchDatasetVersions(ctx, datasets.head.getDid)
    })
  }

  // TODO: change did to name
  @GET
  @Path("/{name}/publicVersion/list")
  def getPublicDatasetVersionList(
      @PathParam("name") did: Integer
  ): List[DatasetVersion] = {
    withTransaction(context)(ctx => {
      if (!isDatasetPublic(ctx, did)) {
        throw new ForbiddenException(ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE)
      }
      fetchDatasetVersions(ctx, did)
    })
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/version/latest")
  def retrieveLatestDatasetVersion(
      @PathParam("did") did: Integer,
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

      val ownerNode = DatasetFileNode
        .fromLakeFSRepositoryCommittedObjects(
          Map(
            (user.getEmail, dataset.getName, latestVersion.getName) ->
              LakeFSFileStorage
                .retrieveObjectsOfVersion(dataset.getName, latestVersion.getVersionHash)
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
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}/version/{dvid}/rootFileNodes")
  def retrieveDatasetVersionRootFileNodes(
      @PathParam("did") did: Integer,
      @PathParam("dvid") dvid: Integer,
      @Auth user: SessionUser
  ): DatasetVersionRootFileNodesResponse = {
    val uid = user.getUid
    withTransaction(context)(ctx =>
      fetchDatasetVersionRootFileNodes(ctx, did, dvid, Some(uid), isPublic = false)
    )
  }

  @GET
  @Path("/{did}/publicVersion/{dvid}/rootFileNodes")
  def retrievePublicDatasetVersionRootFileNodes(
      @PathParam("did") did: Integer,
      @PathParam("dvid") dvid: Integer
  ): DatasetVersionRootFileNodesResponse = {
    withTransaction(context)(ctx =>
      fetchDatasetVersionRootFileNodes(ctx, did, dvid, None, isPublic = true)
    )
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/{did}")
  def getDataset(
      @PathParam("did") did: Integer,
      @Auth user: SessionUser
  ): DashboardDataset = {
    val uid = user.getUid
    withTransaction(context)(ctx => fetchDataset(ctx, did, Some(uid), isPublic = false))
  }

  @GET
  @Path("/public/{did}")
  def getPublicDataset(
      @PathParam("did") did: Integer
  ): DashboardDataset = {
    withTransaction(context)(ctx => fetchDataset(ctx, did, None, isPublic = true))
  }

  @GET
  @Path("/file")
  def retrieveDatasetSingleFile(
      @QueryParam("path") pathStr: String
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
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Path("/version-zip")
  def retrieveDatasetVersionZip(
      @QueryParam("did") did: Integer,
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
      getDatasetVersionByID(context, Integer.valueOf(dvid.get))
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

  @GET
  @Path("/datasetUserAccess")
  def datasetUserAccess(
      @QueryParam("did") did: Integer
  ): java.util.List[Integer] = {
    val records = context
      .select(DATASET_USER_ACCESS.UID)
      .from(DATASET_USER_ACCESS)
      .where(DATASET_USER_ACCESS.DID.eq(did))
      .fetch()

    records.getValues(DATASET_USER_ACCESS.UID)
  }

  private def fetchDatasetVersions(ctx: DSLContext, did: Integer): List[DatasetVersion] = {
    ctx
      .selectFrom(DATASET_VERSION)
      .where(DATASET_VERSION.DID.eq(did))
      .orderBy(DATASET_VERSION.CREATION_TIME.desc()) // Change to .asc() for ascending order
      .fetchInto(classOf[DatasetVersion])
      .asScala
      .toList
  }

  private def fetchDatasetVersionRootFileNodes(
      ctx: DSLContext,
      did: Integer,
      dvid: Integer,
      uid: Option[Integer],
      isPublic: Boolean
  ): DatasetVersionRootFileNodesResponse = {
    val dataset = getDashboardDataset(ctx, did, uid, isPublic)
    val targetDatasetPath = PathUtils.getDatasetPath(did)
    val datasetVersion = getDatasetVersionByID(ctx, dvid)
    val datasetName = dataset.dataset.getName
    val fileNodes = GitVersionControlLocalFileStorage.retrieveRootFileNodesOfVersion(
      targetDatasetPath,
      datasetVersion.getVersionHash
    )
    val versionHash = datasetVersion.getVersionHash
    val size = calculateDatasetVersionSize(datasetName, Some(versionHash))

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
  }

  private def fetchDataset(
      ctx: DSLContext,
      did: Integer,
      uid: Option[Integer],
      isPublic: Boolean
  ): DashboardDataset = {
    val dashboardDataset = getDashboardDataset(ctx, did, uid, isPublic)
    dashboardDataset
  }
}
