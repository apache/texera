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

package edu.uci.ics.amber.util

import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters.IteratorHasAsScala

object PathUtils {
  val coreDirectoryName = "core"

  /**
    * Gets the real path of the workflow-compiling-service home directory by:
    * 1) Checking if the current directory is workflow-compiling-service.
    * If it's not, then:
    * 2) Searching the siblings and children to find the home path.
    *
    * @return the real absolute path to the home directory
    */
  lazy val corePath: Path = {
    val currentWorkingDirectory = Paths.get(".").toRealPath()
    // check if the current directory is the home path
    if (isCorePath(currentWorkingDirectory)) {
      currentWorkingDirectory
    } else {
      // from current path's parent directory, search its children to find home path
      val searchChildren = Files
        .walk(currentWorkingDirectory.getParent, 3)
        .filter((path: Path) => isCorePath(path))
        .findAny
      if (searchChildren.isPresent) {
        searchChildren.get
      } else {
        throw new RuntimeException(
          f"Finding $coreDirectoryName home path failed. Current working directory is " + currentWorkingDirectory
        )
      }
    }
  }

  lazy val workflowCompilingServicePath: Path = corePath.resolve("workflow-compiling-service")

  lazy val fileServicePath: Path = corePath.resolve("file-service")

  lazy val workflowComputingUnitManagingServicePath: Path =
    corePath.resolve("computing-unit-managing-service")

  lazy val configServicePath: Path = corePath.resolve("config-service")

  private lazy val datasetsRootPath =
    corePath.resolve("amber").resolve("user-resources").resolve("datasets")

  def getDatasetPath(did: Integer): Path = {
    datasetsRootPath.resolve(did.toString)
  }

  lazy val gitDirectoryPath: Path = corePath.getParent

  def getAllDatasetDirectories(): List[Path] = {
    if (Files.exists(datasetsRootPath)) {
      Files
        .list(datasetsRootPath)
        .filter(Files.isDirectory(_))
        .iterator()
        .asScala
        .toList
    } else {
      List.empty[Path]
    }
  }

  private def isCorePath(path: Path): Boolean = {
    path.toRealPath().endsWith(coreDirectoryName)
  }
}
