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
import org.apache.texera.amber.core.storage.ResourceType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Unit tests for LakeFSFileNode: the instance helpers getFilePath, the
// constructor's nodeType require guard, and the companion helpers
// calculateTotalSize / fromLakeFSRepositoryCommittedObjects. The LakeFS
// factory is fixtured with lightweight ObjectStats POJOs.
class LakeFSFileNodeSpec extends AnyFlatSpec with Matchers {

  // -- constructor require guard ----------------------------------------------

  "LakeFSFileNode constructor" should "accept nodeType 'file'" in {
    val root = new LakeFSFileNode("/", "directory", null, "")
    val node = new LakeFSFileNode("a.csv", "file", root, "alice", Some(1L))
    node.getNodeType shouldBe "file"
  }

  it should "accept nodeType 'directory'" in {
    val node = new LakeFSFileNode("/", "directory", null, "")
    node.getNodeType shouldBe "directory"
  }

  it should "reject any other nodeType" in {
    val ex = intercept[IllegalArgumentException] {
      new LakeFSFileNode("weird", "symlink", null, "")
    }
    ex.getMessage should include("type must be 'file' or 'directory'")
  }

  // -- getFilePath ------------------------------------------------------------

  "getFilePath" should "return just '/' for the root node" in {
    val root = new LakeFSFileNode("/", "directory", null, "")
    root.getFilePath shouldBe "/"
  }

  it should "walk parents to build an absolute path and skip the root" in {
    val root = new LakeFSFileNode("/", "directory", null, "")
    val a = new LakeFSFileNode("a", "directory", root, "owner")
    val b = new LakeFSFileNode("b", "directory", a, "owner")
    val c = new LakeFSFileNode("c.csv", "file", b, "owner", Some(10L))

    a.getFilePath shouldBe "/a"
    b.getFilePath shouldBe "/a/b"
    c.getFilePath shouldBe "/a/b/c.csv"
  }

  // -- calculateTotalSize -----------------------------------------------------

  "calculateTotalSize" should "return 0 for an empty list" in {
    LakeFSFileNode.calculateTotalSize(List.empty) shouldBe 0L
  }

  it should "sum file sizes recursively across the tree, ignoring directories" in {
    val root = new LakeFSFileNode("/", "directory", null, "")
    val f1 = new LakeFSFileNode("f1", "file", root, "owner", Some(100L))
    val dir = new LakeFSFileNode("dir", "directory", root, "owner")
    val f2 = new LakeFSFileNode("f2", "file", dir, "owner", Some(50L))
    val f3 = new LakeFSFileNode("f3", "file", dir, "owner", Some(25L))
    dir.children = Some(List(f2, f3))

    LakeFSFileNode.calculateTotalSize(List(f1, dir)) shouldBe 175L
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
    val roots = LakeFSFileNode.fromLakeFSRepositoryCommittedObjects(
      ResourceType.Dataset,
      Map(("bob@texera.com", "twitter", "v1") -> objects)
    )

    // The tree is rooted at a single "dataset" prefix node; owners nest under it.
    roots should have size 1
    val prefixNode = roots.head
    prefixNode.getName shouldBe "dataset"
    prefixNode.getNodeType shouldBe "directory"

    val ownerNode = prefixNode.getChildren.find(_.getName == "bob@texera.com").get
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
    file1.getFilePath shouldBe "/dataset/bob@texera.com/twitter/v1/b/1.csv"

    // Total size equals the sum of the three files.
    LakeFSFileNode.calculateTotalSize(roots) shouldBe 6L
  }

  it should "root a model tree at the model prefix, not dataset" in {
    // The prefix is not cosmetic: FileResolver keys on the leading path segment to
    // choose the backing table, so a model file rooted at "dataset" would resolve
    // against the dataset table.
    val roots = LakeFSFileNode.fromLakeFSRepositoryCommittedObjects(
      ResourceType.Model,
      Map(("bob@texera.com", "sentiment", "v1") -> List(objStats("model.pt", 4L)))
    )

    roots should have size 1
    roots.head.getName shouldBe "model"

    val file = roots.head.getChildren.head.getChildren.head.getChildren.head.getChildren.head
    file.getFilePath shouldBe "/model/bob@texera.com/sentiment/v1/model.pt"
  }

  it should "treat a repeated final path segment as one directory and one file" in {
    // "model/model" repeats its last segment. Deciding file-vs-directory by value rather than
    // position made the intermediate directory the leaf: it took the object's size and the real
    // file hung underneath it, so calculateTotalSize double-counted and the frontend saw a
    // "file" node with children.
    val roots = LakeFSFileNode.fromLakeFSRepositoryCommittedObjects(
      ResourceType.Model,
      Map(("bob@texera.com", "sentiment", "v1") -> List(objStats("model/model", 7L)))
    )

    val versionNode =
      roots.head.getChildren.head.getChildren.head.getChildren.head
    val dir = versionNode.getChildren.find(_.getName == "model").get
    dir.getNodeType shouldBe "directory"
    dir.getSize shouldBe None

    val leaf = dir.getChildren.find(_.getName == "model").get
    leaf.getNodeType shouldBe "file"
    leaf.getSize shouldBe Some(7L)
    leaf.getFilePath shouldBe "/model/bob@texera.com/sentiment/v1/model/model"

    // counted once, not twice
    LakeFSFileNode.calculateTotalSize(roots) shouldBe 7L
  }

  it should "not duplicate a name when an object and a directory collide" in {
    // LakeFS is a key-value store: "model" and "model/weights.bin" can both exist in one
    // version. Mapping that onto a tree cannot keep both, but it must not emit two children
    // with the same name.
    val roots = LakeFSFileNode.fromLakeFSRepositoryCommittedObjects(
      ResourceType.Model,
      Map(
        ("bob@texera.com", "sentiment", "v1") -> List(
          objStats("model", 3L),
          objStats("model/weights.bin", 5L)
        )
      )
    )

    val versionNode = roots.head.getChildren.head.getChildren.head.getChildren.head
    val named = versionNode.getChildren.filter(_.getName == "model")
    named should have size 1

    // The directory wins, since it has to hold the deeper object.
    val dir = named.head
    dir.getNodeType shouldBe "directory"
    dir.getSize shouldBe None
    dir.getChildren.map(_.getName) shouldBe List("weights.bin")

    // Only the object that is actually representable is counted.
    LakeFSFileNode.calculateTotalSize(roots) shouldBe 5L
  }
}
