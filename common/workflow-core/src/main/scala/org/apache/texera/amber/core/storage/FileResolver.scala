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

package org.apache.texera.amber.core.storage

import org.apache.commons.vfs2.FileNotFoundException
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.tables.Dataset.DATASET
import org.apache.texera.dao.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, DatasetVersion}

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
    * @throws java.io.FileNotFoundException if the file cannot be resolved.
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
    * @throws java.io.FileNotFoundException if the local file does not exist
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
    * Parses a dataset file path and extracts its components.
    * Expected format: /ownerEmail/datasetName/versionName/fileRelativePath
    *
    * @param fileName The file path to parse
    * @return Some((ownerEmail, datasetName, versionName, fileRelativePath)) if valid, None otherwise
    */
  /** Sentinel that means "resolve to the dataset's latest version".
    * Use it in place of the version segment, e.g. /alice/myDataset/latest/foo.csv
    */
  private val LATEST_VERSION_TOKEN = "latest"

  /**
    * Parse a dataset-file path. Accepts both:
    *   4-segment: /ownerEmail/datasetName/versionName/fileRelativePath
    *   3-segment: /ownerEmail/datasetName/fileRelativePath          (auto-latest)
    * The 3-segment form is signalled internally by versionName == LATEST_VERSION_TOKEN.
    */
  private def parseDatasetFilePath(
      fileName: String
  ): Option[(String, String, String, Array[String])] = {
    val filePath = Paths.get(fileName)
    val pathSegments = (0 until filePath.getNameCount).map(filePath.getName(_).toString).toArray

    if (pathSegments.length < 3) {
      return None
    }

    val ownerEmail = pathSegments(0)
    val datasetName = pathSegments(1)

    // If we have at least 4 segments AND the third segment is NOT "latest", treat it
    // as an explicit version name; otherwise resolve to the dataset's latest version.
    if (pathSegments.length >= 4 && pathSegments(2) != LATEST_VERSION_TOKEN) {
      val versionName = pathSegments(2)
      val fileRelativePathSegments = pathSegments.drop(3)
      Some((ownerEmail, datasetName, versionName, fileRelativePathSegments))
    } else {
      val versionStart = if (pathSegments(2) == LATEST_VERSION_TOKEN) 3 else 2
      val fileRelativePathSegments = pathSegments.drop(versionStart)
      if (fileRelativePathSegments.isEmpty) return None
      Some((ownerEmail, datasetName, LATEST_VERSION_TOKEN, fileRelativePathSegments))
    }
  }

  /**
    * Attempts to resolve a given fileName to a URI.
    *
    * The fileName format should be: /ownerEmail/datasetName/versionName/fileRelativePath
    *   e.g. /bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv
    * The output dataset URI format is: {DATASET_FILE_URI_SCHEME}:///{repositoryName}/{versionHash}/fileRelativePath
    *   e.g. {DATASET_FILE_URI_SCHEME}:///dataset-15/adeq233td/some/dir/file.txt
    *
    * @param fileName the name of the file to attempt resolving as a DatasetFileDocument
    * @return Either[String, DatasetFileDocument] - Right(document) if creation succeeds
    * @throws java.io.FileNotFoundException if the dataset file does not exist or cannot be created
    */
  private def datasetResolveFunc(fileName: String): URI = {
    val (ownerEmail, datasetName, versionName, fileRelativePathSegments) =
      parseDatasetFilePath(fileName).getOrElse(
        throw new FileNotFoundException(s"Dataset file $fileName not found.")
      )

    val fileRelativePath =
      Paths.get(fileRelativePathSegments.head, fileRelativePathSegments.tail: _*)

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

        // fetch the dataset version from DB. If the caller passed the special
        // "latest" token (or the path was 3-segment, which we normalize to "latest"),
        // resolve to the most recently created version of this dataset.
        val datasetVersion = if (versionName == LATEST_VERSION_TOKEN) {
          ctx
            .selectFrom(DATASET_VERSION)
            .where(DATASET_VERSION.DID.eq(dataset.getDid))
            .orderBy(DATASET_VERSION.DVID.desc())
            .limit(1)
            .fetchOneInto(classOf[DatasetVersion])
        } else {
          ctx
            .selectFrom(DATASET_VERSION)
            .where(DATASET_VERSION.DID.eq(dataset.getDid))
            .and(DATASET_VERSION.NAME.eq(versionName))
            .fetchOneInto(classOf[DatasetVersion])
        }

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
      dataset.getRepositoryName,
      datasetVersion.getVersionHash
    ) ++ encodedFileRelativePath

    // Build the format /{repositoryName}/{versionHash}/{fileRelativePath}, both Linux and Windows use forward slash as the splitter
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

  /**
    * Parses a dataset file path to extract owner email and dataset name.
    * Expected format: /ownerEmail/datasetName/versionName/fileRelativePath
    *
    * @param path The file path from operator properties
    * @return Some((ownerEmail, datasetName)) if path is valid, None otherwise
    */
  def parseDatasetOwnerAndName(path: String): Option[(String, String)] = {
    if (path == null) {
      return None
    }
    parseDatasetFilePath(path).map {
      case (ownerEmail, datasetName, _, _) => (ownerEmail, datasetName)
    }
  }
}
