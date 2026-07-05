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

import org.eclipse.jgit.api.Git
import org.scalatest.flatspec.AnyFlatSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Using

class GitVersionControlLocalFileStorageSpec extends AnyFlatSpec {

  private def deleteIfExists(path: Path): Unit = {
    if (Files.exists(path)) {
      Using.resource(Files.walk(path)) { stream =>
        stream.iterator().asScala.toSeq.reverse.foreach { p =>
          // best-effort: JGit can keep Windows handles on .git objects; leftover temp
          // files are harmless and get reaped by the OS.
          try Files.deleteIfExists(p)
          catch { case _: java.io.IOException => () }
        }
      }
    }
  }

  // JGit's commit() does not set an author/committer; configure identity on the
  // temp repo before any commit call, otherwise commit() throws.
  private def setIdentity(repo: Path): Unit = {
    Using.resource(Git.open(repo.toFile)) { git =>
      val config = git.getRepository.getConfig
      config.setString("user", null, "name", "texera-test")
      config.setString("user", null, "email", "t@t")
      config.save()
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

  "GitVersionControlLocalFileStorage" should "support a full versioned lifecycle over a temp repo" in {
    // Create then delete the temp dir so initRepo exercises its createDirectories branch.
    val repoDir = Files.createTempDirectory("texera-vc-lifecycle")
    deleteIfExists(repoDir)
    assert(!Files.exists(repoDir))

    try {
      val branch = GitVersionControlLocalFileStorage.initRepo(repoDir)
      assert(branch != null)
      assert(Files.exists(repoDir.resolve(".git")))
      setIdentity(repoDir)

      // Write a top-level file (hello.txt) into the repo and commit it as version "v1".
      // A nested path would exercise JGitVersionControl.add's OS-separator pathspec, which
      // JGit rejects on Windows; the versioned-storage surface is covered with a root file.
      val filePathInRepo = repoDir.resolve("hello.txt")
      val content = "hello-versioned-content"
      GitVersionControlLocalFileStorage.writeFileToRepo(
        repoDir,
        filePathInRepo,
        new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
      )

      val commitHash = GitVersionControlLocalFileStorage.withCreateVersion(
        repoDir,
        "v1",
        new Runnable { override def run(): Unit = () }
      )
      assert(commitHash.matches("[0-9a-f]{40}"))

      // Root file nodes of the version include the committed hello.txt.
      val rootNodes = GitVersionControlLocalFileStorage
        .retrieveRootFileNodesOfVersion(repoDir, commitHash)
        .asScala
        .toSeq
      val relPaths = PhysicalFileNode.getAllFileRelativePaths(rootNodes.toSet.asJava).asScala
      assert(relPaths.contains("hello.txt"))

      // Retrieve content via OutputStream.
      val out = new ByteArrayOutputStream()
      GitVersionControlLocalFileStorage.retrieveFileContentOfVersion(
        repoDir,
        commitHash,
        filePathInRepo,
        out
      )
      assert(new String(out.toByteArray, StandardCharsets.UTF_8) == content)

      // Retrieve content via InputStream.
      Using.resource(
        GitVersionControlLocalFileStorage.retrieveFileContentOfVersionAsInputStream(
          repoDir,
          commitHash,
          filePathInRepo
        )
      ) { in =>
        assert(new String(in.readAllBytes(), StandardCharsets.UTF_8) == content)
      }

      // Write the versioned file content into a temp file: must be absolute with matching content.
      val tempFile = GitVersionControlLocalFileStorage.writeVersionedFileToTempFile(
        repoDir,
        commitHash,
        filePathInRepo
      )
      try {
        assert(tempFile.isAbsolute)
        assert(new String(Files.readAllBytes(tempFile), StandardCharsets.UTF_8) == content)
      } finally {
        Files.deleteIfExists(tempFile)
      }

      // hasUncommittedChanges / discardUncommittedChanges toggle.
      assert(!GitVersionControlLocalFileStorage.hasUncommittedChanges(repoDir))
      Files.write(filePathInRepo, "dirty".getBytes(StandardCharsets.UTF_8))
      assert(GitVersionControlLocalFileStorage.hasUncommittedChanges(repoDir))
      GitVersionControlLocalFileStorage.discardUncommittedChanges(repoDir)
      assert(!GitVersionControlLocalFileStorage.hasUncommittedChanges(repoDir))
      assert(new String(Files.readAllBytes(filePathInRepo), StandardCharsets.UTF_8) == content)

      // removeFileFromRepo: directory guard then happy-path delete.
      val subDir = repoDir.resolve("subdir")
      Files.createDirectories(subDir)
      val ex = intercept[IllegalArgumentException] {
        GitVersionControlLocalFileStorage.removeFileFromRepo(repoDir, subDir)
      }
      assert(ex.getMessage.contains("directory"))

      GitVersionControlLocalFileStorage.removeFileFromRepo(repoDir, filePathInRepo)
      assert(!Files.exists(filePathInRepo))
    } finally {
      deleteIfExists(repoDir)
    }
  }
}
