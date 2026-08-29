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
  * Unified object for resolving local files and versioned-resource (dataset / model) logical
  * paths to physical URIs.
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
    val resolvers: Seq[String => URI] = Seq(localResolveFunc, versionedResourceResolveFunc)

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
    * Parses a versioned-resource file path into its components, or None if it is not well-formed.
    *
    * Two accepted forms:
    *   - Prefixed:  /<prefix>/ownerEmail/resourceName/versionName/fileRelativePath (>= 5 segments)
    *     where the leading segment names a known [[ResourceType]] (dataset, model, …), so a single
    *     caller can dispatch to the right backing table.
    *   - Legacy unprefixed (datasets only, backward compat): /ownerEmail/datasetName/versionName/
    *     fileRelativePath (>= 4 segments), resolved as a dataset. Models are new and always require
    *     the /model/ prefix.
    *
    * @param fileName the file path to parse
    * @return Some((resourceType, ownerEmail, resourceName, versionName, fileRelativePath)) if valid,
    *         None otherwise
    */
  private def parsePrefixedPath(
      fileName: String
  ): Option[(ResourceType.Value, String, String, String, Array[String])] = {
    val filePath = Paths.get(fileName)
    val pathSegments = (0 until filePath.getNameCount).map(filePath.getName(_).toString).toArray

    pathSegments.headOption.flatMap(ResourceType.fromPrefix) match {
      case Some(resourceType) =>
        // Prefixed: /<prefix>/ownerEmail/resourceName/versionName/<file> (>= 5 segments).
        if (pathSegments.length < 5) None
        else
          Some(
            (resourceType, pathSegments(1), pathSegments(2), pathSegments(3), pathSegments.drop(4))
          )
      case None =>
        // Legacy unprefixed dataset path (backward compat): /ownerEmail/datasetName/versionName/<file>.
        // TODO(dataset-prefix): require the prefix once all stored paths are migrated (36.sql).
        if (pathSegments.length >= 4)
          Some(
            (
              ResourceType.Dataset,
              pathSegments(0),
              pathSegments(1),
              pathSegments(2),
              pathSegments.drop(3)
            )
          )
        else None
    }
  }

  /**
    * Resolves a versioned-resource logical path to its physical `scheme:///` URI.
    *
    * The leading prefix selects the resource kind; only the per-type DB lookup and URI scheme
    * differ, so adding a new versioned resource is a single new `case` below plus a lookup helper.
    * An unprefixed path is resolved as a legacy dataset path (see [[parsePrefixedPath]]).
    *
    * Input:  /<prefix>/ownerEmail/resourceName/versionName/fileRelativePath (or legacy unprefixed)
    * Output: {scheme}:///{repositoryName}/{versionHash}/fileRelativePath
    *   e.g. /dataset/bob@x.com/twitter/v1/dir/f.csv -> dataset:///dataset-15/adeq233td/dir/f.csv
    *        /model/bob@x.com/resnet/v1/weights/m.pt -> model:///model-15/adeq233td/weights/m.pt
    *
    * @throws java.io.FileNotFoundException if the path is not a valid versioned-resource path, the
    *                                       resource/version does not exist, or the URI is malformed
    */
  private def versionedResourceResolveFunc(fileName: String): URI = {
    val (resourceType, ownerEmail, resourceName, versionName, fileRelativePathSegments) =
      parsePrefixedPath(fileName).getOrElse(
        throw new FileNotFoundException(s"Versioned-resource file $fileName not found.")
      )

    val (scheme, repositoryName, versionHash) = resourceType match {
      case ResourceType.Dataset =>
        val (repo, hash) = lookupDataset(ownerEmail, resourceName, versionName, fileName)
        (DATASET_FILE_URI_SCHEME, repo, hash)
      case ResourceType.Model =>
        val (repo, hash) = lookupModel(ownerEmail, resourceName, versionName, fileName)
        (MODEL_FILE_URI_SCHEME, repo, hash)
      case other =>
        throw new FileNotFoundException(s"Unsupported resource type $other for file $fileName.")
    }

    buildVersionedFileURI(scheme, repositoryName, versionHash, fileRelativePathSegments, fileName)
  }

  /**
    * Builds the physical URI {scheme}:///{repositoryName}/{versionHash}/{fileRelativePath},
    * URL-encoding each file-relative-path segment. Uses forward slash on both Linux and Windows.
    */
  private def buildVersionedFileURI(
      scheme: String,
      repositoryName: String,
      versionHash: String,
      fileRelativePathSegments: Array[String],
      fileName: String
  ): URI = {
    val fileRelativePath =
      Paths.get(fileRelativePathSegments.head, fileRelativePathSegments.tail: _*)

    val encodedFileRelativePath = fileRelativePath
      .iterator()
      .asScala
      .map(segment => URLEncoder.encode(segment.toString, StandardCharsets.UTF_8))
      .toArray

    val allPathSegments = Array(repositoryName, versionHash) ++ encodedFileRelativePath
    val encodedPath = "/" + allPathSegments.mkString("/")

    try {
      new URI(scheme, "", encodedPath, null)
    } catch {
      case _: Exception =>
        throw new FileNotFoundException(s"Versioned-resource file $fileName not found.")
    }
  }

  /**
    * Looks up a dataset + version by owner email / name / version name, returning its
    * (repositoryName, versionHash).
    *
    * @throws java.io.FileNotFoundException if the dataset or version does not exist
    */
  private def lookupDataset(
      ownerEmail: String,
      datasetName: String,
      versionName: String,
      fileName: String
  ): (String, String) =
    withTransaction(
      SqlServer
        .getInstance()
        .createDSLContext()
    ) { ctx =>
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

  /**
    * Looks up a model + version by owner email / name / version name, returning its
    * (repositoryName, versionHash). Mirrors [[lookupDataset]] against the model tables.
    *
    * @throws java.io.FileNotFoundException if the model or version does not exist
    */
  private def lookupModel(
      ownerEmail: String,
      modelName: String,
      versionName: String,
      fileName: String
  ): (String, String) =
    withTransaction(
      SqlServer
        .getInstance()
        .createDSLContext()
    ) { ctx =>
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
    parsePrefixedPath(path).collect {
      case (ResourceType.Dataset, ownerEmail, datasetName, _, _) => (ownerEmail, datasetName)
    }
  }
}
