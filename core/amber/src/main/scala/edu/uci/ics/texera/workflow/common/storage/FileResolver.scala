package edu.uci.ics.texera.workflow.common.storage

import edu.uci.ics.amber.engine.common.Utils.withTransaction
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.web.model.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import edu.uci.ics.texera.web.model.jooq.generated.tables.User.USER
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{Dataset, DatasetVersion}
import org.apache.commons.vfs2.FileNotFoundException

import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Success, Try}

object FileResolver {
  val DATASET_FILE_URI_SCHEME = "vfs"

  /**
    * Attempts to resolve the given fileName using a list of resolver functions.
    *
    * @param fileName the name of the file to resolve
    * @throws FileNotFoundException if the file cannot be resolved by any resolver
    * @return Either[String, DatasetFileDocument] - the resolved path as a String or a DatasetFileDocument
    */
  def resolve(fileName: String): URI = {
    val resolvers: Seq[String => URI] = Seq(localResolveFunc, datasetResolveFunc)

    // Try each resolver function in sequence
    resolvers
      .map(resolver => Try(resolver(fileName)))
      .collectFirst {
        case Success(output) => output
      }
      .getOrElse(throw new FileNotFoundException(fileName))
  }

  /**
    * Attempts to resolve a local file path.
    * @throws FileNotFoundException if the local file does not exist
    * @param fileName the name of the file to check
    */
  private def localResolveFunc(fileName: String): URI = {
    val filePath = Paths.get(fileName)
    if (!Files.exists(filePath)) {
      throw new FileNotFoundException(s"Local file $fileName does not exist")
    }
    filePath.toUri
  }

  /**
    * Attempts to resolve a given fileName to a URI.
    *
    * The fileName format should be: /ownerEmail/datasetName/versionName/fileRelativePath
    *   e.g. /bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv
    * The output dataset URI format is: {DATASET_FILE_URI_SCHEME}:///{did}/{versionHash}/file-path
    *   e.g. vfs:///15/adeq233td/some/dir/file.txt
    *
    * @param fileName the name of the file to attempt resolving as a DatasetFileDocument
    * @return Either[String, DatasetFileDocument] - Right(document) if creation succeeds
    * @throws FileNotFoundException if the dataset file does not exist or cannot be created
    */
  private def datasetResolveFunc(fileName: String): URI = {
    val filePath = Paths.get(fileName)
    val pathSegments = (0 until filePath.getNameCount).map(filePath.getName(_).toString).toArray

    // extract info from the user-given fileName
    val ownerEmail = pathSegments(0)
    val datasetName = pathSegments(1)
    val versionName = pathSegments(2)
    val fileRelativePath = Paths.get(pathSegments.drop(3).head, pathSegments.drop(3).tail: _*)

    // fetch the dataset and version from DB to get dataset ID and version hash
    val (dataset, datasetVersion) =
      withTransaction(SqlServer.createDSLContext()) { ctx =>
        // fetch the dataset from DB
        val dataset = ctx
          .select(DATASET.fields: _*)
          .from(DATASET)
          .leftJoin(USER)
          .on(USER.UID.eq(DATASET.OWNER_UID))
          .where(USER.EMAIL.eq(ownerEmail))
          .and(DATASET.NAME.eq(datasetName))
          .fetchOneInto(classOf[Dataset])

        // fetch the dataset version from DB
        val datasetVersion = ctx
          .selectFrom(DATASET_VERSION)
          .where(DATASET_VERSION.DID.eq(dataset.getDid))
          .and(DATASET_VERSION.NAME.eq(versionName))
          .fetchOneInto(classOf[DatasetVersion])

        if (dataset == null || datasetVersion == null) {
          throw new FileNotFoundException(s"Dataset file $fileName not found.")
        }
        (dataset, datasetVersion)
      }

    // Convert each segment of fileRelativePath to an encoded String
    val encodedFileRelativePath = fileRelativePath
      .iterator()
      .asScala
      .map { segment =>
        URLEncoder.encode(segment.toString, StandardCharsets.UTF_8)
      }
      .toArray

    // Prepend did and versionHash to the encoded path segments
    val allPathSegments = Array(
      dataset.getDid.intValue().toString,
      datasetVersion.getVersionHash
    ) ++ encodedFileRelativePath

    // Build the the format /{did}/{versionHash}/{fileRelativePath}, both Linux and Windows use forward slash as the splitter
    val uriSplitter = "/"
    val encodedPath = uriSplitter + allPathSegments.mkString(uriSplitter)

    try {
      new URI(DATASET_FILE_URI_SCHEME, "", encodedPath, null)
    } catch {
      case e: Exception =>
        throw new FileNotFoundException(s"Dataset file $fileName not found.")
    }
  }
}
