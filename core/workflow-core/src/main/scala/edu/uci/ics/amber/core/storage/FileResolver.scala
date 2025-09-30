/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.amber.core.storage

import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.SqlServer.withTransaction
import edu.uci.ics.texera.dao.jooq.generated.tables.Dataset.DATASET
import edu.uci.ics.texera.dao.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import edu.uci.ics.texera.dao.jooq.generated.tables.User.USER
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{Dataset, DatasetVersion}
import org.apache.commons.vfs2.FileNotFoundException

import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Success, Try}

/**
  * Unified object for resolving both VFS resources and local/dataset files.
  */
object FileResolver {

  val DATASET_FILE_URI_SCHEME = "dataset"

  /**
    * Resolves a given fileName to either a file on the local file system or a dataset file.
    *
    * @param fileName the name of the file to resolve.
    * @throws FileNotFoundException if the file cannot be resolved.
    * @return A URI pointing to the resolved file.
    */
  def resolve(fileName: String): URI = {
    if (isFileResolved(fileName)) {
      return new URI(fileName)
    }
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
    *   e.g. {DATASET_FILE_URI_SCHEME}:///15/adeq233td/some/dir/file.txt
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
      withTransaction(
        SqlServer
          .getInstance()
          .createDSLContext()
      ) { ctx =>
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

    // Prepend dataset name and versionHash to the encoded path segments
    val allPathSegments = Array(
      datasetName,
      datasetVersion.getVersionHash
    ) ++ encodedFileRelativePath

    // Build the format /{datasetName}/{versionHash}/{fileRelativePath}, both Linux and Windows use forward slash as the splitter
    val uriSplitter = "/"
    val encodedPath = uriSplitter + allPathSegments.mkString(uriSplitter)

    try {
      new URI(DATASET_FILE_URI_SCHEME, "", encodedPath, null)
    } catch {
      case e: Exception =>
        throw new FileNotFoundException(s"Dataset file $fileName not found.")
    }
  }

  /**
    * Checks if a given file path has a valid scheme.
    *
    * @param filePath The file path to check.
    * @return `true` if the file path contains a valid scheme, `false` otherwise.
    */
  def isFileResolved(filePath: String): Boolean = {
    try {
      val uri = new URI(filePath)
      uri.getScheme != null && uri.getScheme.nonEmpty
    } catch {
      case _: Exception => false // Invalid URI format
    }
  }
}
