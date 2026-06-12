/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.core.storage.util.dataset

import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Using

class GitVersionControlLocalFileStorageSpec extends AnyFlatSpec {

  private def deleteIfExists(path: Path): Unit = {
    if (Files.exists(path)) {
      Using.resource(Files.walk(path)) { stream =>
        stream.iterator().asScala.toSeq.reverse.foreach(Files.deleteIfExists)
      }
    }
  }

  "GitVersionControlLocalFileStorage.deleteRepo" should "delete a repository directory recursively" in {
    val repoDir = Files.createTempDirectory("texera-delete-repo-test")

    try {
      val nestedDir = Files.createDirectories(repoDir.resolve("nested").resolve("child"))
      val rootFile = Files.writeString(repoDir.resolve("root.txt"), "root")
      val nestedFile = Files.writeString(nestedDir.resolve("data.txt"), "data")

      assert(Files.exists(repoDir))
      assert(Files.exists(rootFile))
      assert(Files.exists(nestedFile))

      GitVersionControlLocalFileStorage.deleteRepo(repoDir)

      assert(!Files.exists(repoDir))
    } finally {
      deleteIfExists(repoDir)
    }
  }
}
