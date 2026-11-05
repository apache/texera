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
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.Dataset.DATASET
import org.apache.texera.dao.jooq.generated.tables.DatasetUserAccess.DATASET_USER_ACCESS
import org.apache.texera.dao.jooq.generated.tables.DatasetVersion.DATASET_VERSION
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, DatasetVersion}
import org.jooq.DSLContext

import java.net.{URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.control.NonFatal

/**
  * Resolves dataset logical paths to physical URIs.
  */
object FileResolver {

  val DATASET_FILE_URI_SCHEME = "dataset"

  /**
    * Returns a [[ScanFileResolver]] scoped to the user `uid`.
    */
  def forUser(uid: Integer): ScanFileResolver = fileName => resolveForUser(fileName, uid)

  /**
    * Resolves a dataset logical path for the user `uid`.
    *
    * @throws java.io.FileNotFoundException if the path does not resolve to a readable dataset file
    */
  def resolveForUser(fileName: String, uid: Integer): URI =
    resolveLogicalPath(fileName, reader = Some(uid))

  /**
    * Resolves a dataset logical path without a user scope.
    *
    * @throws java.io.FileNotFoundException if the path does not resolve to a dataset file
    */
  def resolveVersionedResource(fileName: String): URI =
    resolveLogicalPath(fileName, reader = None)

  private def resolveLogicalPath(fileName: String, reader: Option[Integer]): URI =
    try {
      if (isFileResolved(fileName)) {
        throw new FileNotFoundException(fileName)
      }
      datasetResolveFunc(fileName, reader)
    } catch {
      case NonFatal(_) => throw new FileNotFoundException(fileName)
    }

  /**
    * Parses a dataset file path and extracts its components.
    * Expected format: /ownerEmail/datasetName/versionName/fileRelativePath
    *
    * @param fileName The file path to parse
    * @return Some((ownerEmail, datasetName, versionName, fileRelativePath)) if valid, None otherwise
    */
  private def parseDatasetFilePath(
      fileName: String
  ): Option[(String, String, String, Array[String])] = {
    val filePath = Paths.get(fileName)
    val pathSegments = (0 until filePath.getNameCount).map(filePath.getName(_).toString).toArray

    if (pathSegments.length < 4) {
      return None
    }

    val ownerEmail = pathSegments(0)
    val datasetName = pathSegments(1)
    val versionName = pathSegments(2)
    val fileRelativePathSegments = pathSegments.drop(3)

    Some((ownerEmail, datasetName, versionName, fileRelativePathSegments))
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
    * @param reader optional user ID to check read access for
    * @return Either[String, DatasetFileDocument] - Right(document) if creation succeeds
    * @throws java.io.FileNotFoundException if the dataset file does not exist, cannot be created,
    *                                       or the reader does not have read access
    */
  private def datasetResolveFunc(fileName: String, reader: Option[Integer]): URI = {
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

        if (dataset == null || !readableBy(ctx, dataset, reader)) {
          throw new FileNotFoundException(s"Dataset file $fileName not found.")
        }

        // fetch the dataset version from DB
        val datasetVersion = ctx
          .selectFrom(DATASET_VERSION)
          .where(DATASET_VERSION.DID.eq(dataset.getDid))
          .and(DATASET_VERSION.NAME.eq(versionName))
          .fetchOneInto(classOf[DatasetVersion])

        if (datasetVersion == null) {
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

  /** Checks whether `reader` has read access when a user scope is provided. */
  private def readableBy(ctx: DSLContext, dataset: Dataset, reader: Option[Integer]): Boolean =
    reader.forall(uid =>
      dataset.getIsPublic || dataset.getOwnerUid == uid || ctx.fetchExists(
        DATASET_USER_ACCESS,
        DATASET_USER_ACCESS.DID
          .eq(dataset.getDid)
          .and(DATASET_USER_ACCESS.UID.eq(uid))
          .and(DATASET_USER_ACCESS.PRIVILEGE.in(PrivilegeEnum.READ, PrivilegeEnum.WRITE))
      )
    )

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
