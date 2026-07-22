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

package org.apache.texera.service.`type`

import io.lakefs.clients.sdk.model.ObjectStats
import org.apache.texera.amber.core.storage.util.dataset.PhysicalFileNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

// Unit tests for DatasetFileNode: the instance helpers getFilePath, the
// constructor's nodeType require guard, and the companion helpers
// calculateTotalSize / fromLakeFSRepositoryCommittedObjects /
// fromPhysicalFileNodes. The LakeFS factory is fixtured with lightweight
// ObjectStats POJOs; the physical-node factory is fixtured against a real
// temporary directory tree because it inspects the filesystem.
class DatasetFileNodeSpec extends AnyFlatSpec with Matchers {

  // -- constructor require guard ----------------------------------------------

  "DatasetFileNode constructor" should "accept nodeType 'file'" in {
    val root = new DatasetFileNode("/", "directory", null, "")
    val node = new DatasetFileNode("a.csv", "file", root, "alice", Some(1L))
    node.getNodeType shouldBe "file"
  }

  it should "accept nodeType 'directory'" in {
    val node = new DatasetFileNode("/", "directory", null, "")
    node.getNodeType shouldBe "directory"
  }

  it should "reject any other nodeType" in {
    val ex = intercept[IllegalArgumentException] {
      new DatasetFileNode("weird", "symlink", null, "")
    }
    ex.getMessage should include("type must be 'file' or 'directory'")
  }

  // -- getFilePath ------------------------------------------------------------

  "getFilePath" should "return just '/' for the root node" in {
    val root = new DatasetFileNode("/", "directory", null, "")
    root.getFilePath shouldBe "/"
  }

  it should "walk parents to build an absolute path and skip the root" in {
    val root = new DatasetFileNode("/", "directory", null, "")
    val a = new DatasetFileNode("a", "directory", root, "owner")
    val b = new DatasetFileNode("b", "directory", a, "owner")
    val c = new DatasetFileNode("c.csv", "file", b, "owner", Some(10L))

    a.getFilePath shouldBe "/a"
    b.getFilePath shouldBe "/a/b"
    c.getFilePath shouldBe "/a/b/c.csv"
  }

  // -- calculateTotalSize -----------------------------------------------------

  "calculateTotalSize" should "return 0 for an empty list" in {
    DatasetFileNode.calculateTotalSize(List.empty) shouldBe 0L
  }

  it should "sum file sizes recursively across the tree, ignoring directories" in {
    val root = new DatasetFileNode("/", "directory", null, "")
    val f1 = new DatasetFileNode("f1", "file", root, "owner", Some(100L))
    val dir = new DatasetFileNode("dir", "directory", root, "owner")
    val f2 = new DatasetFileNode("f2", "file", dir, "owner", Some(50L))
    val f3 = new DatasetFileNode("f3", "file", dir, "owner", Some(25L))
    dir.children = Some(List(f2, f3))

    DatasetFileNode.calculateTotalSize(List(f1, dir)) shouldBe 175L
  }

  // -- fromLakeFSRepositoryCommittedObjects -----------------------------------

  private def objStats(path: String, size: Long): ObjectStats =
    new ObjectStats().path(path).sizeBytes(size)

  "fromLakeFSRepositoryCommittedObjects" should "build a sorted owner/dataset/version tree" in {
    val objects = List(
      objStats("a/x.csv", 1L),
      objStats("b/1.csv", 2L),
      objStats("b/2.csv", 3L)
    )
    val roots = DatasetFileNode.fromLakeFSRepositoryCommittedObjects(
      Map(("bob@texera.com", "twitter", "v1") -> objects)
    )

    // One owner root.
    roots should have size 1
    val ownerNode = roots.head
    ownerNode.getName shouldBe "bob@texera.com"
    ownerNode.getNodeType shouldBe "directory"

    val datasetNode = ownerNode.getChildren.find(_.getName == "twitter").get
    val versionNode = datasetNode.getChildren.find(_.getName == "v1").get

    // Top-level dirs under the version are sorted by name descending: b before a.
    versionNode.getChildren.map(_.getName) shouldBe List("b", "a")

    // Directory "b" is created once (dedup) and holds both files, sorted descending.
    val bDir = versionNode.getChildren.find(_.getName == "b").get
    bDir.getNodeType shouldBe "directory"
    bDir.getChildren.map(_.getName) shouldBe List("2.csv", "1.csv")

    // Leaf sizes and full paths are wired up correctly.
    val file1 = bDir.getChildren.find(_.getName == "1.csv").get
    file1.getNodeType shouldBe "file"
    file1.getSize shouldBe Some(2L)
    file1.getFilePath shouldBe "/bob@texera.com/twitter/v1/b/1.csv"

    // Total size equals the sum of the three files.
    DatasetFileNode.calculateTotalSize(roots) shouldBe 6L
  }

  // -- fromPhysicalFileNodes --------------------------------------------------

  "fromPhysicalFileNodes" should "build a tree from a physical filesystem subtree" in {
    // Create a real temp tree: <repo>/dir1/a.csv , because PhysicalFileNode
    // uses Files.isDirectory / Files.isRegularFile against the actual path.
    val repo: Path = Files.createTempDirectory("dataset-file-node-spec")
    try {
      val dir1 = Files.createDirectory(repo.resolve("dir1"))
      val fileA = Files.write(dir1.resolve("a.csv"), "hello".getBytes)

      val dir1Node = new PhysicalFileNode(repo, dir1, 0L)
      // getSize returns the value passed at construction, not the on-disk size.
      val fileNode = new PhysicalFileNode(repo, fileA, 5L)
      dir1Node.addChildNode(fileNode)

      val roots = DatasetFileNode.fromPhysicalFileNodes(
        Map(("alice@texera.com", "ds", "v1") -> List(dir1Node))
      )

      roots should have size 1
      val versionNode = roots.head.getChildren
        .find(_.getName == "ds")
        .get
        .getChildren
        .find(_.getName == "v1")
        .get

      val dirNode = versionNode.getChildren.find(_.getName == "dir1").get
      dirNode.getNodeType shouldBe "directory"

      val leaf = dirNode.getChildren.find(_.getName == "a.csv").get
      leaf.getNodeType shouldBe "file"
      leaf.getSize shouldBe Some(5L)
      leaf.getFilePath shouldBe "/alice@texera.com/ds/v1/dir1/a.csv"

      DatasetFileNode.calculateTotalSize(roots) shouldBe 5L
    } finally {
      // Best-effort cleanup of the temp tree.
      val stream = Files.walk(repo)
      try {
        stream
          .sorted(java.util.Comparator.reverseOrder[Path]())
          .iterator()
          .asScala
          .foreach(p => Files.deleteIfExists(p))
      } finally {
        stream.close()
      }
    }
  }
}
