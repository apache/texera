package edu.uci.ics.texera.web.resource.dashboard.user.dataset

import edu.uci.ics.texera.Utils.withTransaction
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.web.model.jooq.generated.enums.DatasetUserAccessPrivilege
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{
  DatasetDao,
  DatasetUserAccessDao,
  DatasetVersionDao,
  UserDao
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{
  Dataset,
  DatasetUserAccess,
  DatasetVersion
}
import edu.uci.ics.texera.web.model.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.web.model.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.DatasetAccessResource.{
  getDatasetUserAccessPrivilege,
  getOwner,
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
  DatasetVersionFileTree,
  DatasetVersions,
  context,
  getDashboardDataset,
  getDatasetByID,
  getDatasetLatestVersion,
  getDatasetVersionHashByID,
  persistNewVersion,
  withExceptionHandling
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.error.{
  DatasetVersionNotFoundException,
  ResourceNotExistsException,
  UserHasNoAccessToDatasetException
}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.storage.{LocalFileStorage, PathUtils}
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.version.GitVersionControl
import io.dropwizard.auth.Auth
import org.glassfish.jersey.media.multipart.{FormDataMultiPart, FormDataParam}
import org.jooq.{DSLContext, EnumType}
import org.jooq.types.UInteger

import java.io.{InputStream, OutputStream}
import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.security.RolesAllowed
import javax.ws.rs.{
  BadRequestException,
  Consumes,
  GET,
  InternalServerErrorException,
  POST,
  Path,
  PathParam,
  Produces,
  QueryParam
}
import javax.ws.rs.core.{MediaType, Response, StreamingOutput}
import scala.jdk.CollectionConverters._

object DatasetResource {
  val PUBLIC: Byte = 1;
  val PRIVATE: Byte = 0;

  val FILE_OPERATION_UPLOAD_PREFIX = "file:upload:"
  val FILE_OPERATION_REMOVE_PREFIX = "file:remove"

  val datasetLocks: scala.collection.concurrent.Map[UInteger, ReentrantLock] =
    new scala.collection.concurrent.TrieMap[UInteger, ReentrantLock]()

  def withExceptionHandling[T](block: () => T): T = {
    try {
      block()
    } catch {
      case e: BadRequestException =>
        throw e
      case e: Exception =>
        // Optionally log the full exception here for debugging purposes
        throw new InternalServerErrorException(
          Option(e.getMessage).getOrElse("An unknown error occurred.")
        )
    }
  }

  private val context = SqlServer.createDSLContext()

  private def getDatasetByID(ctx: DSLContext, did: UInteger, uid: UInteger): Dataset = {
    if (!userHasReadAccess(ctx, did, uid)) {
      throw new UserHasNoAccessToDatasetException(did.intValue())
    }
    val datasetDao = new DatasetDao(ctx.configuration())
    val dataset = datasetDao.fetchOneByDid(did)
    if (dataset == null) {
      throw new ResourceNotExistsException("dataset")
    }
    dataset
  }

  private def getDatasetVersionHashByID(
      ctx: DSLContext,
      did: UInteger,
      dvid: UInteger,
      uid: UInteger
  ): String = {
    if (!userHasReadAccess(ctx, did, uid)) {
      throw new UserHasNoAccessToDatasetException(did.intValue())
    }
    val datasetVersionDao = new DatasetVersionDao(ctx.configuration())
    val version = datasetVersionDao.fetchOneByDvid(dvid)
    if (version == null) {
      throw new ResourceNotExistsException("dataset version")
    }
    version.getVersionHash
  }

  def getDashboardDataset(ctx: DSLContext, did: UInteger, uid: UInteger): DashboardDataset = {
    if (!userHasReadAccess(ctx, did, uid)) {
      throw new UserHasNoAccessToDatasetException(did.intValue())
    }

    val targetDataset = getDatasetByID(ctx, did, uid)
    val userAccessPrivilege = getDatasetUserAccessPrivilege(ctx, did, uid)

    DashboardDataset(
      targetDataset,
      userAccessPrivilege,
      targetDataset.getOwnerUid == uid
    )
  }

  def getDatasetLatestVersion(ctx: DSLContext, did: UInteger, uid: UInteger): DatasetVersion = {
    if (!userHasReadAccess(ctx, did, uid)) {
      throw new UserHasNoAccessToDatasetException(did.intValue())
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
      throw new DatasetVersionNotFoundException(did.intValue())
    }

    latestVersion
  }

  private def persistNewVersion(
      ctx: DSLContext,
      did: UInteger,
      uid: UInteger,
      versionName: String,
      multiPart: FormDataMultiPart
  ): Option[DashboardDatasetVersion] = {

    // Acquire the lock
    val lock = DatasetResource.datasetLocks.getOrElseUpdate(did, new ReentrantLock())

    if (lock.isLocked) {
      return None
    }
    lock.lock()
    try {
      val datasetPath = PathUtils.getDatasetPath(did).toString

      val gitVersionControl = new GitVersionControl(datasetPath)
      val fileStorage = new LocalFileStorage(datasetPath)

      var fileOperationHappens = false
      // for multipart, each file-related operation's key starts with file:
      // the operation is either upload or remove
      // for file:upload, the file path will be suffixed to it, e.g. file:upload:a/b/c.csv The value will be the file content
      // for file:remove, the value would be filepath1,filepath2
      val fields = multiPart.getFields().keySet().iterator()
      while (fields.hasNext) {
        val fieldName = fields.next()
        val bodyPart = multiPart.getField(fieldName)

        if (fieldName.startsWith(FILE_OPERATION_UPLOAD_PREFIX)) {
          //        val contentDisposition = bodyPart.getContentDisposition
          //        val contentType = bodyPart.getMediaType.toString
          val filePath = fieldName.substring(FILE_OPERATION_UPLOAD_PREFIX.length)
          val value: InputStream = bodyPart.getValueAs(classOf[InputStream])
          fileStorage.addFile(filePath, value)
          fileOperationHappens = true
        } else if (fieldName.startsWith(FILE_OPERATION_REMOVE_PREFIX)) {
          val filePathsValue = bodyPart.getValueAs(classOf[String])
          val filePaths = filePathsValue.split(",")
          filePaths.foreach { filePath =>
            fileStorage.removeFile(filePath.trim)
          }
          fileOperationHappens = true
        }
      }

      if (!fileOperationHappens) {
        return None
      }

      val commitHash = gitVersionControl.createVersion(versionName)
      val datasetVersion = new DatasetVersion()

      datasetVersion.setName(versionName)
      datasetVersion.setDid(did)
      datasetVersion.setCreatorUid(uid)
      datasetVersion.setVersionHash(commitHash)

      Some(
        DashboardDatasetVersion(
          ctx
            .insertInto(DATASET_VERSION) // Assuming DATASET is the table reference
            .set(ctx.newRecord(DATASET_VERSION, datasetVersion))
            .returning() // Assuming ID is the primary key column
            .fetchOne()
            .into(classOf[DatasetVersion]),
          gitVersionControl.retrieveFileTreeOfVersion(commitHash)
        )
      )
    } finally {
      // Release the lock
      lock.unlock()
    }
  }

  case class DashboardDataset(
      dataset: Dataset,
      accessPrivilege: EnumType,
      isOwner: Boolean
  )

  case class DatasetVersionFileTree(fileTree: util.Map[String, AnyRef])

  case class DatasetVersions(versions: List[DatasetVersion])

  case class DashboardDatasetVersion(
      datasetVersion: DatasetVersion,
      fileTree: util.Map[String, AnyRef]
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

    withExceptionHandling { () =>
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
        val datasetPath = PathUtils.getDatasetPath(did).toString
        createdDataset.setStoragePath(datasetPath)
        createdDataset.update()

        val datasetUserAccess = new DatasetUserAccess()
        datasetUserAccess.setDid(createdDataset.getDid)
        datasetUserAccess.setUid(uid)
        datasetUserAccess.setPrivilege(DatasetUserAccessPrivilege.WRITE)
        datasetOfUserDao.insert(datasetUserAccess)

        // create the initial version of the dataset
        // init the dataset dir
        val datasetFileStorage = new LocalFileStorage(datasetPath)
        datasetFileStorage.initDir()
        val datasetVersionControl = new GitVersionControl(datasetPath)
        datasetVersionControl.initRepo()

        val createdVersion = persistNewVersion(ctx, did, uid, initialVersionName, files)

        createdVersion match {
          case Some(_) =>
          case None    =>
            // none means creation failed
            throw new BadRequestException("User should do modifications to create a new version")
        }

        DashboardDataset(
          new Dataset(
            createdDataset.getDid,
            createdDataset.getOwnerUid,
            createdDataset.getName,
            createdDataset.getIsPublic,
            createdDataset.getStoragePath,
            createdDataset.getDescription,
            createdDataset.getCreationTime,
          ),
          DatasetUserAccessPrivilege.WRITE,
          isOwner = true
        )
      }
    }
  }

  @POST
  @Path("/delete")
  def deleteDataset(datasetIDs: DatasetIDs, @Auth user: SessionUser): Response = {
    val uid = user.getUid
    withExceptionHandling { () =>
      withTransaction(context) { ctx =>
        val datasetDao = new DatasetDao(ctx.configuration())
        for (did <- datasetIDs.dids) {
          if (!userOwnDataset(ctx, did, uid)) {
            // throw the exception that user has no access to certain dataset
            throw new UserHasNoAccessToDatasetException(did.intValue())
          }
          val dataset = getDatasetByID(ctx, did, uid)
          val datasetStorage = new LocalFileStorage(dataset.getStoragePath)
          datasetStorage.remove()
          datasetDao.deleteById(did)
        }

        Response.ok().build()
      }
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
    withExceptionHandling { () =>
      withTransaction(context) { ctx =>
        val datasetDao = new DatasetDao(ctx.configuration())
        val uid = sessionUser.getUid
        val did = modificator.did
        val name = modificator.name
        if (!userHasWriteAccess(ctx, did, uid)) {
          throw new UserHasNoAccessToDatasetException(did.intValue())
        }

        val existedDataset = getDatasetByID(ctx, did, uid)
        existedDataset.setName(name)
        datasetDao.update(existedDataset)
        Response.ok().build()
      }
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
    withExceptionHandling { () =>
      withTransaction(context) { ctx =>
        val datasetDao = new DatasetDao(ctx.configuration())
        val uid = sessionUser.getUid
        val did = modificator.did
        val description = modificator.description

        if (!userHasWriteAccess(ctx, did, uid)) {
          throw new UserHasNoAccessToDatasetException(did.intValue())
        }

        val existedDataset = getDatasetByID(ctx, did, uid)
        existedDataset.setDescription(description)
        datasetDao.update(existedDataset)
        Response.ok().build()
      }
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
    withExceptionHandling({ () =>
      withTransaction(context) { ctx =>
        if (!userHasWriteAccess(ctx, did, uid)) {
          // throw the exception that user has no access to certain dataset
          throw new UserHasNoAccessToDatasetException(did.intValue())
        }
        // create the version
        val createdVersion = persistNewVersion(ctx, did, uid, versionName, multiPart)

        createdVersion match {
          case None =>
            throw new BadRequestException("User should do modifications to create a new version")
          case Some(version) => version
        }
      }
    })
  }

  @GET
  @Path("/{did}")
  def getDataset(
      @PathParam("did") did: UInteger,
      @Auth user: SessionUser
  ): DashboardDataset = {
    val uid = user.getUid
    withExceptionHandling({ () =>
      withTransaction(context)(ctx => {
        getDashboardDataset(ctx, did, uid)
      })
    })
  }

  @GET
  @Path("/{did}/version/list")
  def getDatasetVersionList(
      @PathParam("did") did: UInteger,
      @Auth user: SessionUser
  ): DatasetVersions = {
    val uid = user.getUid
    withExceptionHandling({ () =>
      withTransaction(context)(ctx => {

        if (!userHasReadAccess(ctx, did, uid)) {
          throw new UserHasNoAccessToDatasetException(did.intValue())
        }
        val result: java.util.List[DatasetVersion] = ctx
          .selectFrom(DATASET_VERSION)
          .where(DATASET_VERSION.DID.eq(did))
          .orderBy(DATASET_VERSION.CREATION_TIME.desc()) // or .asc() for ascending
          .fetchInto(classOf[DatasetVersion])

        DatasetVersions(result.asScala.toList)
      })
    })
  }

  @GET
  @Path("/{did}/version/latest")
  def getLatestDatasetVersion(
      @PathParam("did") did: UInteger,
      @Auth user: SessionUser
  ): DashboardDatasetVersion = {
    val uid = user.getUid
    withExceptionHandling({ () =>
      withTransaction(context)(ctx => {
        val latestVersion = getDatasetLatestVersion(ctx, did, uid)

        val gitVersionControl =
          new GitVersionControl(PathUtils.getDatasetPath(latestVersion.getDid).toString)
        DashboardDatasetVersion(
          latestVersion,
          gitVersionControl.retrieveFileTreeOfVersion(latestVersion.getVersionHash)
        )
      })
    })
  }

  @GET
  @Path("/{did}/version/{dvid}/fileTree")
  def retrieveDatasetVersionFileTree(
      @PathParam("did") did: UInteger,
      @PathParam("dvid") dvid: UInteger,
      @Auth user: SessionUser
  ): DatasetVersionFileTree = {
    val uid = user.getUid
    withExceptionHandling({ () =>
      {
        withTransaction(context)(ctx => {
          val targetDataset = getDatasetByID(ctx, did, uid)
          val targetDatasetStoragePath = targetDataset.getStoragePath
          val versionCommitHash = getDatasetVersionHashByID(ctx, did, dvid, uid)
          val gitVersionControl = new GitVersionControl(targetDatasetStoragePath)

          val fileTree = gitVersionControl.retrieveFileTreeOfVersion(versionCommitHash)
          DatasetVersionFileTree(fileTree)
        })
      }
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
    withExceptionHandling({ () =>
      withTransaction(context)(ctx => {
        val decodedPath = URLDecoder.decode(path, StandardCharsets.UTF_8.name()).stripPrefix("/")

        val targetDataset = getDatasetByID(ctx, did, uid)
        val targetDatasetStoragePath = targetDataset.getStoragePath

        val versionCommitHash = getDatasetVersionHashByID(ctx, did, dvid, uid)
        val gitVersionControl = new GitVersionControl(targetDatasetStoragePath)

        val streamingOutput = new StreamingOutput() {
          override def write(output: OutputStream): Unit = {
            gitVersionControl.retrieveFileContentOfVersion(versionCommitHash, decodedPath, output)
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
    })
  }

}
