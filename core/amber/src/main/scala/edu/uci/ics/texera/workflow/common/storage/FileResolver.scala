package edu.uci.ics.texera.workflow.common.storage

import edu.uci.ics.amber.engine.common.Utils.withTransaction

import java.nio.file.{Files, Path, Paths}
import edu.uci.ics.amber.engine.common.storage.{DatasetFileDocument, ReadonlyLocalFileDocument, ReadonlyVirtualDocument, VirtualDocument}
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{Dataset, DatasetVersion}
import edu.uci.ics.texera.web.model.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.web.model.jooq.generated.tables.User.USER
import edu.uci.ics.texera.web.model.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import org.apache.commons.vfs2.FileNotFoundException
import org.jooq.DSLContext

import java.net.URI
import scala.util.{Success, Try}

object FileResolver {
  private val DatasetFileUriScheme = "vfs"

  /**
    * Attempts to resolve the given fileName using a list of resolver functions.
    *
    * @param fileName the name of the file to resolve
    * @throws FileNotFoundException if the file cannot be resolved by any resolver
    * @return Either[String, DatasetFileDocument] - the resolved path as a String or a DatasetFileDocument
    */
  def resolve(fileName: String): URI = {
    val resolvers: List[String => URI] = List(localResolveFunc, datasetResolveFunc)

    // Try each resolver function in sequence
    resolvers.iterator
      .map(resolver => Try(resolver(fileName)))
      .collectFirst {
        case Success(output) => output
      }
      .getOrElse(throw new FileNotFoundException(fileName))
  }

  /**
    * Open a file handle for the given fileUri
    * @param fileUri the uri pointing to the file
    * @return
    */
  def open(fileUri: URI): ReadonlyVirtualDocument[_] = {
    fileUri.getScheme match {
      case DatasetFileUriScheme =>
        // Parse the host to get dataset ID and version hash
        val hostParts = fileUri.getHost.split("\\.")
        if (hostParts.length != 2) {
          throw new RuntimeException(s"Invalid dataset URI format: ${fileUri.toString}")
        }
        val datasetId = hostParts(0).toInt
        val versionHash = hostParts(1)

        // The path within the URI represents the relative path of the file in the dataset
        val fileRelativePath = Paths.get(fileUri.getPath.stripPrefix("/"))

        // Create and return a DatasetFileDocument with the parsed values
        new DatasetFileDocument(datasetId, versionHash, fileRelativePath)

      case "file" =>
        // For local files, create a ReadonlyLocalFileDocument
        new ReadonlyLocalFileDocument(fileUri)

      case _ =>
        throw new UnsupportedOperationException(s"Unsupported URI scheme: ${fileUri.getScheme}")
    }
  }

  /**
    * Attempts to resolve a local file path.
    * @throws FileNotFoundException if the local file does not exist
    * @param fileName the name of the file to check
    */
  private def localResolveFunc(fileName: String): URI = {
    val filePath = Paths.get(fileName)
    if (Files.exists(filePath)) {
      filePath.toUri // File exists locally, return the path as a string in the Left
    } else {
      throw new FileNotFoundException(s"Local file $fileName does not exist")
    }
  }

  /**
    * Attempts to resolve a given fileName to a URI.
    *
    * The fileName format should be: /ownerEmail/datasetName/versionName/fileRelativePath
    *   e.g. /bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv
    * The output dataset URI format is: {DatasetFileUriScheme}://{did}.{versionHash}/file-path
    *   e.g. vfs://15.adeq233td/some/dir/file.txt
    *
    * @param fileName the name of the file to attempt resolving as a DatasetFileDocument
    * @return Either[String, DatasetFileDocument] - Right(document) if creation succeeds
    * @throws FileNotFoundException if the dataset file does not exist or cannot be created
    */
  private def datasetResolveFunc(fileName: String): URI = {
    withTransaction(SqlServer.createDSLContext()) { ctx =>
      val (_, dataset, datasetVersion, fileRelativePath) = parseFileNameForDataset(ctx, fileName)
      if (dataset == null || datasetVersion == null) {
        throw new FileNotFoundException(s"Dataset file $fileName")
      }

      // assemble dataset URI format
      val host = s"${dataset.getDid.intValue()}.${datasetVersion.getVersionHash}"
      new URI(DatasetFileUriScheme, host, fileRelativePath.toUri.getPath, null)
    }
  }

  def parseFileNameForDataset(ctx: DSLContext, fileName: String): (String, Dataset, DatasetVersion, Path) = {
    val filePath = Paths.get(fileName)
    val pathSegments = (0 until filePath.getNameCount).map(filePath.getName(_).toString).toArray

    val ownerEmail = pathSegments(0)
    val datasetName = pathSegments(1)
    val versionName = pathSegments(2)
    val fileRelativePath = Paths.get(pathSegments.drop(3).mkString("/"))

    val dataset = ctx
      .select(DATASET.fields: _*)
      .from(DATASET)
      .leftJoin(USER)
      .on(USER.UID.eq(DATASET.OWNER_UID))
      .where(USER.EMAIL.eq(ownerEmail))
      .and(DATASET.NAME.eq(datasetName))
      .fetchOneInto(classOf[Dataset])

    val datasetVersion = ctx
      .selectFrom(DATASET_VERSION)
      .where(DATASET_VERSION.DID.eq(dataset.getDid))
      .and(DATASET_VERSION.NAME.eq(versionName))
      .fetchOneInto(classOf[DatasetVersion])
    (ownerEmail, dataset, datasetVersion, fileRelativePath)
  }
}
