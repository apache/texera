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
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayOutputStream, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Using

class JGitVersionControlSpec extends AnyFlatSpec with Matchers {

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

  private def writeFile(path: Path, content: String): Unit = {
    Files.createDirectories(path.getParent)
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  // Stage every working-tree change portably. JGitVersionControl.add relativizes with the
  // OS path separator, and JGit's pathspec matcher rejects the Windows "dir\\file" form; the
  // "." pattern walks the tree emitting correct forward-slash entries on every platform, so
  // it is used to stage nested files as fixture setup.
  private def stageAll(repo: Path): Unit =
    Using.resource(Git.open(repo.toFile))(_.add().addFilepattern(".").call())

  "JGitVersionControl.initRepo" should "initialize a repo and return a non-null default branch" in {
    val repo = Files.createTempDirectory("texera-jgit-init")
    try {
      val branch = JGitVersionControl.initRepo(repo)
      branch should not be null
      Files.exists(repo.resolve(".git")) shouldBe true
    } finally {
      deleteIfExists(repo)
    }
  }

  it should "throw IOException when a repository already exists" in {
    val repo = Files.createTempDirectory("texera-jgit-init-exists")
    try {
      JGitVersionControl.initRepo(repo)
      val ex = intercept[IOException] {
        JGitVersionControl.initRepo(repo)
      }
      ex.getMessage should include("Repository already exists")
    } finally {
      deleteIfExists(repo)
    }
  }

  "JGitVersionControl.commit" should "stage, commit, and return a 40-char hex hash" in {
    val repo = Files.createTempDirectory("texera-jgit-commit")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)

      val file = repo.resolve("top.txt")
      writeFile(file, "hello")
      JGitVersionControl.add(repo, file)

      val hash = JGitVersionControl.commit(repo, "initial commit")
      hash should fullyMatch regex "[0-9a-f]{40}"
    } finally {
      deleteIfExists(repo)
    }
  }

  "JGitVersionControl.getRootFileNodeOfCommit" should "return top-level and nested nodes with size and relative path" in {
    val repo = Files.createTempDirectory("texera-jgit-tree")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)

      val topFile = repo.resolve("top.txt")
      val topContent = "top-content"
      writeFile(topFile, topContent)

      val nestedFile = repo.resolve("dir").resolve("nested.txt")
      val nestedContent = "nested-file-content"
      writeFile(nestedFile, nestedContent)

      JGitVersionControl.add(repo, topFile)
      stageAll(repo) // stage the nested file portably (see stageAll)
      val hash = JGitVersionControl.commit(repo, "add files")

      val rootNodes = JGitVersionControl.getRootFileNodeOfCommit(repo, hash).asScala.toSeq

      // Root level should contain the top-level file and the "dir" directory node.
      val relPaths = rootNodes.map(_.getRelativePath.toString).toSet
      relPaths should contain("top.txt")
      relPaths should contain("dir")

      val topNode = rootNodes.find(_.getRelativePath.toString == "top.txt").get
      topNode.isDirectory shouldBe false
      topNode.getSize shouldBe topContent.getBytes(StandardCharsets.UTF_8).length.toLong

      val dirNode = rootNodes.find(_.getRelativePath.toString == "dir").get
      dirNode.isDirectory shouldBe true

      val childNode = dirNode.getChildren.asScala.head
      childNode.getRelativePath.toString should (be("dir/nested.txt") or be("dir\\nested.txt"))
      childNode.isDirectory shouldBe false
      childNode.getSize shouldBe nestedContent.getBytes(StandardCharsets.UTF_8).length.toLong
    } finally {
      deleteIfExists(repo)
    }
  }

  "JGitVersionControl read methods" should "round-trip file content via InputStream and OutputStream" in {
    val repo = Files.createTempDirectory("texera-jgit-read")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)

      val file = repo.resolve("data.txt")
      val content = "round-trip-content"
      writeFile(file, content)
      JGitVersionControl.add(repo, file)
      val hash = JGitVersionControl.commit(repo, "add data")

      // OutputStream variant
      val out = new ByteArrayOutputStream()
      JGitVersionControl.readFileContentOfCommitAsOutputStream(repo, hash, file, out)
      new String(out.toByteArray, StandardCharsets.UTF_8) shouldBe content

      // InputStream variant
      Using.resource(JGitVersionControl.readFileContentOfCommitAsInputStream(repo, hash, file)) {
        in =>
          val bytes = in.readAllBytes()
          new String(bytes, StandardCharsets.UTF_8) shouldBe content
      }
    } finally {
      deleteIfExists(repo)
    }
  }

  "JGitVersionControl.readFileContentOfCommitAsInputStream" should "reject a path outside the repository" in {
    val repo = Files.createTempDirectory("texera-jgit-guard-in-out")
    val outside = Files.createTempDirectory("texera-jgit-guard-in-outside")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)
      val file = repo.resolve("data.txt")
      writeFile(file, "x")
      JGitVersionControl.add(repo, file)
      val hash = JGitVersionControl.commit(repo, "c")

      intercept[IllegalArgumentException] {
        JGitVersionControl.readFileContentOfCommitAsInputStream(
          repo,
          hash,
          outside.resolve("f.txt")
        )
      }
    } finally {
      deleteIfExists(repo)
      deleteIfExists(outside)
    }
  }

  it should "reject a path that points to a directory" in {
    val repo = Files.createTempDirectory("texera-jgit-guard-in-dir")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)
      val dir = repo.resolve("dir")
      val file = dir.resolve("nested.txt")
      writeFile(file, "x")
      JGitVersionControl.add(repo, file)
      val hash = JGitVersionControl.commit(repo, "c")

      intercept[IllegalArgumentException] {
        JGitVersionControl.readFileContentOfCommitAsInputStream(repo, hash, dir)
      }
    } finally {
      deleteIfExists(repo)
    }
  }

  it should "throw IOException for a file not present in the commit" in {
    val repo = Files.createTempDirectory("texera-jgit-guard-in-missing")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)
      val file = repo.resolve("data.txt")
      writeFile(file, "x")
      JGitVersionControl.add(repo, file)
      val hash = JGitVersionControl.commit(repo, "c")

      intercept[IOException] {
        JGitVersionControl.readFileContentOfCommitAsInputStream(
          repo,
          hash,
          repo.resolve("missing.txt")
        )
      }
    } finally {
      deleteIfExists(repo)
    }
  }

  "JGitVersionControl.readFileContentOfCommitAsOutputStream" should "reject a path outside the repository" in {
    val repo = Files.createTempDirectory("texera-jgit-guard-out-out")
    val outside = Files.createTempDirectory("texera-jgit-guard-out-outside")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)
      val file = repo.resolve("data.txt")
      writeFile(file, "x")
      JGitVersionControl.add(repo, file)
      val hash = JGitVersionControl.commit(repo, "c")

      intercept[IllegalArgumentException] {
        JGitVersionControl.readFileContentOfCommitAsOutputStream(
          repo,
          hash,
          outside.resolve("f.txt"),
          new ByteArrayOutputStream()
        )
      }
    } finally {
      deleteIfExists(repo)
      deleteIfExists(outside)
    }
  }

  it should "reject a path that points to a directory" in {
    val repo = Files.createTempDirectory("texera-jgit-guard-out-dir")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)
      val dir = repo.resolve("dir")
      val file = dir.resolve("nested.txt")
      writeFile(file, "x")
      JGitVersionControl.add(repo, file)
      val hash = JGitVersionControl.commit(repo, "c")

      intercept[IllegalArgumentException] {
        JGitVersionControl.readFileContentOfCommitAsOutputStream(
          repo,
          hash,
          dir,
          new ByteArrayOutputStream()
        )
      }
    } finally {
      deleteIfExists(repo)
    }
  }

  it should "throw IOException for a file not present in the commit" in {
    val repo = Files.createTempDirectory("texera-jgit-guard-out-missing")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)
      val file = repo.resolve("data.txt")
      writeFile(file, "x")
      JGitVersionControl.add(repo, file)
      val hash = JGitVersionControl.commit(repo, "c")

      intercept[IOException] {
        JGitVersionControl.readFileContentOfCommitAsOutputStream(
          repo,
          hash,
          repo.resolve("missing.txt"),
          new ByteArrayOutputStream()
        )
      }
    } finally {
      deleteIfExists(repo)
    }
  }

  "JGitVersionControl.rm" should "stage a file deletion so a subsequent commit drops the file" in {
    val repo = Files.createTempDirectory("texera-jgit-rm")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)

      val file = repo.resolve("data.txt")
      writeFile(file, "content")
      JGitVersionControl.add(repo, file)
      JGitVersionControl.commit(repo, "add")

      JGitVersionControl.rm(repo, file)
      val hash = JGitVersionControl.commit(repo, "remove")

      val rootNodes = JGitVersionControl.getRootFileNodeOfCommit(repo, hash).asScala.toSeq
      rootNodes.map(_.getRelativePath.toString) should not contain "data.txt"
    } finally {
      deleteIfExists(repo)
    }
  }

  "JGitVersionControl.hasUncommittedChanges" should "return false on a clean repo and true after a change" in {
    val repo = Files.createTempDirectory("texera-jgit-status")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)

      val file = repo.resolve("data.txt")
      writeFile(file, "content")
      JGitVersionControl.add(repo, file)
      JGitVersionControl.commit(repo, "add")

      JGitVersionControl.hasUncommittedChanges(repo) shouldBe false

      writeFile(repo.resolve("new.txt"), "new")
      JGitVersionControl.hasUncommittedChanges(repo) shouldBe true
    } finally {
      deleteIfExists(repo)
    }
  }

  "JGitVersionControl.discardUncommittedChanges" should "revert the working directory to the last commit" in {
    val repo = Files.createTempDirectory("texera-jgit-discard")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)

      val file = repo.resolve("data.txt")
      writeFile(file, "original")
      JGitVersionControl.add(repo, file)
      JGitVersionControl.commit(repo, "add")

      // Modify tracked file and add an untracked file
      writeFile(file, "modified")
      writeFile(repo.resolve("untracked.txt"), "junk")
      JGitVersionControl.hasUncommittedChanges(repo) shouldBe true

      JGitVersionControl.discardUncommittedChanges(repo)

      JGitVersionControl.hasUncommittedChanges(repo) shouldBe false
      new String(Files.readAllBytes(file), StandardCharsets.UTF_8) shouldBe "original"
      Files.exists(repo.resolve("untracked.txt")) shouldBe false
    } finally {
      deleteIfExists(repo)
    }
  }

  it should "return the branch name that the new repository actually has checked out" in {
    val repo = Files.createTempDirectory("texera-jgit-branch")
    try {
      val branch = JGitVersionControl.initRepo(repo)
      // initRepo strips the "refs/heads/" prefix off the HEAD symref; the result must
      // be the very branch JGit reports for the fresh repository.
      val actual = Using.resource(Git.open(repo.toFile))(_.getRepository.getBranch)
      branch shouldBe actual
      branch should not startWith "refs/heads/"
    } finally {
      deleteIfExists(repo)
    }
  }

  "JGitVersionControl.getRootFileNodeOfCommit" should "return an empty set for a commit with no files" in {
    val repo = Files.createTempDirectory("texera-jgit-empty-tree")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)
      // an initial commit with nothing staged has an empty tree, so the tree walk
      // yields no entries at all
      val hash = JGitVersionControl.commit(repo, "empty commit")

      JGitVersionControl.getRootFileNodeOfCommit(repo, hash).asScala shouldBe empty
    } finally {
      deleteIfExists(repo)
    }
  }

  it should "link nested directories at depth two down to the leaf file" in {
    val repo = Files.createTempDirectory("texera-jgit-deep-tree")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)

      val leafContent = "deep-content"
      writeFile(repo.resolve("a").resolve("b").resolve("leaf.txt"), leafContent)
      stageAll(repo)
      val hash = JGitVersionControl.commit(repo, "add nested dirs")

      val rootNodes = JGitVersionControl.getRootFileNodeOfCommit(repo, hash).asScala.toSeq
      // only the top-level directory "a" is a root node; everything else hangs off it
      rootNodes.map(_.getRelativePath.toString) shouldBe Seq("a")

      val aNode = rootNodes.head
      aNode.isDirectory shouldBe true
      aNode.getChildren.size shouldBe 1

      val bNode = aNode.getChildren.asScala.head
      bNode.getRelativePath.toString should (be("a/b") or be("a\\b"))
      bNode.isDirectory shouldBe true
      bNode.getChildren.size shouldBe 1

      val leafNode = bNode.getChildren.asScala.head
      leafNode.getRelativePath.toString should (be("a/b/leaf.txt") or be("a\\b\\leaf.txt"))
      leafNode.isDirectory shouldBe false
      leafNode.getSize shouldBe leafContent.getBytes(StandardCharsets.UTF_8).length.toLong
    } finally {
      deleteIfExists(repo)
    }
  }

  "JGitVersionControl commit-hash resolution" should "accept a symbolic ref such as HEAD" in {
    val repo = Files.createTempDirectory("texera-jgit-symref")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)
      val file = repo.resolve("data.txt")
      writeFile(file, "head-content")
      JGitVersionControl.add(repo, file)
      JGitVersionControl.commit(repo, "add")

      // the commitHash argument is resolved by JGit, so any revision expression works
      val out = new ByteArrayOutputStream()
      JGitVersionControl.readFileContentOfCommitAsOutputStream(repo, "HEAD", file, out)
      new String(out.toByteArray, StandardCharsets.UTF_8) shouldBe "head-content"

      Using.resource(JGitVersionControl.readFileContentOfCommitAsInputStream(repo, "HEAD", file)) {
        in => new String(in.readAllBytes(), StandardCharsets.UTF_8) shouldBe "head-content"
      }
    } finally {
      deleteIfExists(repo)
    }
  }

  it should "read the version stored in each commit after a tracked file is modified" in {
    val repo = Files.createTempDirectory("texera-jgit-history")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)

      val file = repo.resolve("data.txt")
      writeFile(file, "v1")
      JGitVersionControl.add(repo, file)
      val firstHash = JGitVersionControl.commit(repo, "v1")

      // re-staging a tracked file picks up the modification
      writeFile(file, "v2-longer")
      JGitVersionControl.add(repo, file)
      val secondHash = JGitVersionControl.commit(repo, "v2")

      secondHash should not be firstHash

      def contentAt(hash: String): String = {
        val out = new ByteArrayOutputStream()
        JGitVersionControl.readFileContentOfCommitAsOutputStream(repo, hash, file, out)
        new String(out.toByteArray, StandardCharsets.UTF_8)
      }
      contentAt(firstHash) shouldBe "v1"
      contentAt(secondHash) shouldBe "v2-longer"

      // the node size reported for the latest commit follows the new content
      val node = JGitVersionControl
        .getRootFileNodeOfCommit(repo, secondHash)
        .asScala
        .find(_.getRelativePath.toString == "data.txt")
        .get
      node.getSize shouldBe "v2-longer".length.toLong
    } finally {
      deleteIfExists(repo)
    }
  }

  "JGitVersionControl.readFileContentOfCommitAsOutputStream" should "copy binary content byte-for-byte" in {
    val repo = Files.createTempDirectory("texera-jgit-binary")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)

      val bytes = Array[Byte](0, 1, 127, -128, -1, 13, 10, 0)
      val file = repo.resolve("blob.bin")
      Files.write(file, bytes)
      JGitVersionControl.add(repo, file)
      val hash = JGitVersionControl.commit(repo, "add binary")

      val out = new ByteArrayOutputStream()
      JGitVersionControl.readFileContentOfCommitAsOutputStream(repo, hash, file, out)
      out.toByteArray shouldBe bytes

      Using.resource(JGitVersionControl.readFileContentOfCommitAsInputStream(repo, hash, file)) {
        in => in.readAllBytes() shouldBe bytes
      }
    } finally {
      deleteIfExists(repo)
    }
  }

  "JGitVersionControl.hasUncommittedChanges" should "report a staged deletion as uncommitted" in {
    val repo = Files.createTempDirectory("texera-jgit-staged-rm")
    try {
      JGitVersionControl.initRepo(repo)
      setIdentity(repo)

      val file = repo.resolve("data.txt")
      writeFile(file, "content")
      JGitVersionControl.add(repo, file)
      JGitVersionControl.commit(repo, "add")
      JGitVersionControl.hasUncommittedChanges(repo) shouldBe false

      // rm only stages the removal; the index now differs from HEAD
      JGitVersionControl.rm(repo, file)
      JGitVersionControl.hasUncommittedChanges(repo) shouldBe true
      Files.exists(file) shouldBe false

      JGitVersionControl.commit(repo, "remove")
      JGitVersionControl.hasUncommittedChanges(repo) shouldBe false
    } finally {
      deleteIfExists(repo)
    }
  }
}
