package edu.uci.ics.amber.storage.dataset

import edu.uci.ics.amber.util.ResourceUtils.withTransaction
import edu.uci.ics.amber.util.PathUtils

import edu.uci.ics.texera.SqlServer
import edu.uci.ics.texera.model.jooq.generated.tables.pojos.{Dataset, DatasetVersion}
import edu.uci.ics.texera.model.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.model.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import edu.uci.ics.texera.model.jooq.generated.tables.User.USER
import edu.uci.ics.texera.model.jooq.generated.tables.daos.{DatasetDao, DatasetVersionDao}
import org.jooq.DSLContext
import org.jooq.tools.StringUtils
import org.jooq.types.UInteger

import java.io.InputStream
import java.nio.file.{Path, Paths}
import jakarta.ws.rs.{NotFoundException, BadRequestException}

object DatasetResource {

  private val context = SqlServer.createDSLContext()

  // error messages
  val ERR_USER_HAS_NO_ACCESS_TO_DATASET_MESSAGE = "User has no read access to this dataset"
  val ERR_DATASET_NOT_FOUND_MESSAGE = "Dataset not found"
  val ERR_DATASET_VERSION_NOT_FOUND_MESSAGE = "The version of the dataset not found"

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
      throw new NotFoundException(ERR_DATASET_VERSION_NOT_FOUND_MESSAGE)
    }
    version
  }

  private def getDatasetPath(
      did: UInteger
  ): Path = {
    PathUtils.userResourcesConfigPath.resolve("datasets").resolve(did.toString)
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
        throw new NotFoundException(ERR_DATASET_NOT_FOUND_MESSAGE)
      }

      // Get the dataset version by dataset ID and version name
      val datasetVersion = getDatasetVersionByName(ctx, dataset.getDid, versionName)
      if (datasetVersion == null) {
        throw new NotFoundException(ERR_DATASET_VERSION_NOT_FOUND_MESSAGE)
      }

      (ownerEmail, dataset, datasetVersion, fileRelativePath)
    }
  }

  def getDatasetFile(
      did: UInteger,
      dvid: UInteger,
      fileRelativePath: java.nio.file.Path
  ): InputStream = {
    val versionHash = getDatasetVersionByID(context, dvid).getVersionHash
    val datasetPath = getDatasetPath(did)
    GitVersionControlLocalFileStorage
      .retrieveFileContentOfVersionAsInputStream(
        datasetPath,
        versionHash,
        datasetPath.resolve(fileRelativePath)
      )
  }
}
