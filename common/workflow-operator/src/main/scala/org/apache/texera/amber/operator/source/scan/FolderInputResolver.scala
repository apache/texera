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

package org.apache.texera.amber.operator.source.scan

import org.apache.texera.amber.core.storage.{DocumentFactory, FileResolver}
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient

import java.net.{URI, URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import scala.jdk.CollectionConverters._
import scala.util.Using

case class ResolvedInputFile(uri: URI, displayName: String)
case class ResolvedFolderInput(files: List[ResolvedInputFile], isFolder: Boolean)

object FolderInputResolver {

  def resolve(uri: URI): ResolvedFolderInput =
    Option(uri.getScheme).map(_.toLowerCase) match {
      case Some("file")                               => resolveLocalInput(uri)
      case Some(FileResolver.DATASET_FILE_URI_SCHEME) => resolveDatasetInput(uri)
      case _ =>
        ResolvedFolderInput(List(ResolvedInputFile(uri, uri.toASCIIString)), isFolder = false)
    }

  /**
   * Return a real local path that Python libraries can open. Local file-system folders are already
   * usable as-is; dataset-backed folders need to be materialized into a temporary local directory
   * because they only exist as a set of object-store files behind a virtual Texera path.
   */
  def materializeToLocalPath(uri: URI): Path = {
    if (Option(uri.getScheme).contains("file") && Files.isDirectory(Paths.get(uri))) {
      return Paths.get(uri)
    }

    val resolved = resolve(uri)
    if (!resolved.isFolder) {
      DocumentFactory.openReadonlyDocument(uri).asFile().toPath
    } else {
      val root = Files.createTempDirectory("texera-folder-input-")
      resolved.files.foreach { file =>
        val target = root.resolve(file.displayName)
        Option(target.getParent).foreach(parent => Files.createDirectories(parent))
        Using.resource(DocumentFactory.openReadonlyDocument(file.uri).asInputStream()) { in =>
          Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING)
        }
      }
      root
    }
  }

  private def resolveLocalInput(uri: URI): ResolvedFolderInput = {
    val path = Paths.get(uri)
    if (Files.isDirectory(path)) {
      val files = Using.resource(Files.walk(path)) { stream =>
        stream
          .iterator()
          .asScala
          .filter(Files.isRegularFile(_))
          .filterNot(isHiddenPath)
          .map(file => ResolvedInputFile(file.toUri, path.relativize(file).toString))
          .toList
          .sortBy(_.displayName)
      }
      ResolvedFolderInput(files, isFolder = true)
    } else {
      ResolvedFolderInput(List(ResolvedInputFile(uri, uri.toASCIIString)), isFolder = false)
    }
  }

  private def resolveDatasetInput(uri: URI): ResolvedFolderInput = {
    val segments = Paths
      .get(uri.getPath)
      .iterator()
      .asScala
      .map(_.toString)
      .toList

    if (segments.length < 3) {
      throw new IllegalArgumentException(s"Dataset URI is missing a relative path: $uri")
    }

    val repositoryName = segments.head
    val versionHash = URLDecoder.decode(segments(1), StandardCharsets.UTF_8)
    val relativePath = segments
      .drop(2)
      .map(part => URLDecoder.decode(part, StandardCharsets.UTF_8))
      .mkString("/")

    val objects = LakeFSStorageClient.retrieveObjectsOfVersion(repositoryName, versionHash)
    val exactFile = objects.find(_.getPath == relativePath)
    exactFile match {
      case Some(file) =>
        ResolvedFolderInput(
          List(
            ResolvedInputFile(
              buildDatasetFileUri(repositoryName, versionHash, file.getPath),
              uri.toASCIIString
            )
          ),
          isFolder = false
        )
      case None =>
        val prefix = if (relativePath.endsWith("/")) relativePath else s"$relativePath/"
        val files = objects
          .map(_.getPath)
          .filter(_.startsWith(prefix))
          .filterNot(isHiddenDatasetPath)
          .sorted
          .map { path =>
            ResolvedInputFile(
              buildDatasetFileUri(repositoryName, versionHash, path),
              path.stripPrefix(prefix)
            )
          }
        ResolvedFolderInput(files, isFolder = true)
    }
  }

  private def buildDatasetFileUri(repositoryName: String, versionHash: String, relativePath: String): URI = {
    val encodedSegments =
      List(repositoryName, versionHash) ++ relativePath
        .split("/")
        .toList
        .filter(_.nonEmpty)
        .map(segment => URLEncoder.encode(segment, StandardCharsets.UTF_8))
    new URI(FileResolver.DATASET_FILE_URI_SCHEME, "", s"/${encodedSegments.mkString("/")}", null)
  }

  private def isHiddenPath(path: Path): Boolean =
    Option(path.getFileName).exists(_.toString.startsWith("."))

  private def isHiddenDatasetPath(path: String): Boolean =
    path.split("/").lastOption.exists(_.startsWith("."))
}
