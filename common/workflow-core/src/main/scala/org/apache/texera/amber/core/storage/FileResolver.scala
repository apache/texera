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
import org.apache.texera.dao.jooq.generated.tables.Model.MODEL
import org.apache.texera.dao.jooq.generated.tables.ModelVersion.MODEL_VERSION
import org.apache.texera.dao.jooq.generated.tables.User.USER
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  Dataset,
  DatasetVersion,
  Model,
  ModelVersion
}

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
  val MODEL_FILE_URI_SCHEME = "model"

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
    val resolvers: Seq[String => URI] = Seq(localResolveFunc, datasetResolveFunc, modelResolveFunc)

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
    * Parses a versioned-resource file path of the form
    * /<prefix>/ownerEmail/resourceName/versionName/fileRelativePath and extracts its components.
    *
    * @param resourceType the resource-type segment the path must start with
    * @param fileName The file path to parse
    * @return Some((ownerEmail, resourceName, versionName, fileRelativePath)) if valid, None otherwise
    */
  private def parsePrefixedPath(
      resourceType: ResourceType.Value,
      fileName: String
  ): Option[(String, String, String, Array[String])] = {
    val filePath = Paths.get(fileName)
    val pathSegments = (0 until filePath.getNameCount).map(filePath.getName(_).toString).toArray

    if (pathSegments.length < 5 || pathSegments(0) != resourceType.toString) {
      return None
    }

    val ownerEmail = pathSegments(1)
    val resourceName = pathSegments(2)
    val versionName = pathSegments(3)
    val fileRelativePathSegments = pathSegments.drop(4)

    Some((ownerEmail, resourceName, versionName, fileRelativePathSegments))
  }

  private def parseDatasetFilePath(
      fileName: String
  ): Option[(String, String, String, Array[String])] =
    parsePrefixedPath(ResourceType.Datasets, fileName)

  /**
    * Resolves a versioned-resource logical path to its physical `scheme:///` URI.
    *
    * @throws java.io.FileNotFoundException if the path is not a valid `resourceType` path, the
    *                                       resource/version does not exist, or the URI is malformed
    */
  private def resolveVersionedFile(
      scheme: String,
      resourceType: ResourceType.Value,
      fileName: String,
      notFoundMessage: String
  )(lookup: (String, String, String) => (String, String)): URI = {
    val (ownerEmail, resourceName, versionName, fileRelativePathSegments) =
      parsePrefixedPath(resourceType, fileName).getOrElse(
        throw new FileNotFoundException(notFoundMessage)
      )

    val (repositoryName, versionHash) = lookup(ownerEmail, resourceName, versionName)

    val fileRelativePath =
      Paths.get(fileRelativePathSegments.head, fileRelativePathSegments.tail: _*)

    // Convert each segment of fileRelativePath to an encoded String
    val encodedFileRelativePath = fileRelativePath
      .iterator()
      .asScala
      .map { segment =>
        URLEncoder.encode(segment.toString, StandardCharsets.UTF_8)
      }
      .toArray

    // Prepend repositoryName and versionHash to the encoded path segments
    val allPathSegments = Array(repositoryName, versionHash) ++ encodedFileRelativePath

    // Build the format /{repositoryName}/{versionHash}/{fileRelativePath}, both Linux and Windows use forward slash as the splitter
    val uriSplitter = "/"
    val encodedPath = uriSplitter + allPathSegments.mkString(uriSplitter)

    try {
      new URI(scheme, "", encodedPath, null)
    } catch {
      case _: Exception =>
        throw new FileNotFoundException(notFoundMessage)
    }
  }

  /**
    * Attempts to resolve a dataset file path to a URI.
    *
    * The fileName format should be: /datasets/ownerEmail/datasetName/versionName/fileRelativePath
    *   e.g. /datasets/bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv
    * The output dataset URI format is: {DATASET_FILE_URI_SCHEME}:///{repositoryName}/{versionHash}/fileRelativePath
    *   e.g. {DATASET_FILE_URI_SCHEME}:///dataset-15/adeq233td/some/dir/file.txt
    *
    * @param fileName the name of the file to attempt resolving as a DatasetFileDocument
    * @throws java.io.FileNotFoundException if the dataset file does not exist or cannot be created
    */
  private def datasetResolveFunc(fileName: String): URI =
    resolveVersionedFile(
      DATASET_FILE_URI_SCHEME,
      ResourceType.Datasets,
      fileName,
      s"Dataset file $fileName not found."
    ) { (ownerEmail, datasetName, versionName) =>
      // fetch the dataset and version from DB to get the repository name and version hash
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

        // fail early if the dataset does not exist (before dereferencing it below)
        if (dataset == null) {
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
        (dataset.getRepositoryName, datasetVersion.getVersionHash)
      }
    }

  /**
    * Attempts to resolve a model file path to a URI.
    *
    * The fileName format should be: /models/ownerEmail/modelName/versionName/fileRelativePath
    *   e.g. /models/bob@texera.com/resnet/v1/weights/model.pt
    * The output model URI format is: {MODEL_FILE_URI_SCHEME}:///{repositoryName}/{versionHash}/fileRelativePath
    *   e.g. {MODEL_FILE_URI_SCHEME}:///model-15/adeq233td/weights/model.pt
    *
    * @param fileName the name of the file to attempt resolving as a ModelFileDocument
    * @throws java.io.FileNotFoundException if the model file does not exist or cannot be created
    */
  private def modelResolveFunc(fileName: String): URI =
    resolveVersionedFile(
      MODEL_FILE_URI_SCHEME,
      ResourceType.Models,
      fileName,
      s"Model file $fileName not found."
    ) { (ownerEmail, modelName, versionName) =>
      // fetch the model and version from DB to get the repository name and version hash
      withTransaction(
        SqlServer
          .getInstance()
          .createDSLContext()
      ) { ctx =>
        // fetch the model from DB
        val model = ctx
          .select(MODEL.fields: _*)
          .from(MODEL)
          .leftJoin(USER)
          .on(USER.UID.eq(MODEL.OWNER_UID))
          .where(USER.EMAIL.eq(ownerEmail))
          .and(MODEL.NAME.eq(modelName))
          .fetchOneInto(classOf[Model])

        // fail early if the model does not exist (before dereferencing it below)
        if (model == null) {
          throw new FileNotFoundException(s"Model file $fileName not found.")
        }

        // fetch the model version from DB
        val modelVersion = ctx
          .selectFrom(MODEL_VERSION)
          .where(MODEL_VERSION.MID.eq(model.getMid))
          .and(MODEL_VERSION.NAME.eq(versionName))
          .fetchOneInto(classOf[ModelVersion])

        if (modelVersion == null) {
          throw new FileNotFoundException(s"Model file $fileName not found.")
        }
        (model.getRepositoryName, modelVersion.getVersionHash)
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
    * Extracts the owner email and dataset name from a dataset logical path,
    * or None if it is not a well-formed dataset path.
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
