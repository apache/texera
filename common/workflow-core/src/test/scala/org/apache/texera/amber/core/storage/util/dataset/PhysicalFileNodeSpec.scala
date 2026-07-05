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

package org.apache.texera.amber.core.storage.util.dataset

import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Using

class PhysicalFileNodeSpec extends AnyFlatSpec {

  private def withRepo(body: Path => Unit): Unit = {
    val repo = Files.createTempDirectory("pfn-")
    try body(repo)
    finally {
      // Files.walk returns a Stream holding a directory handle; close it so temp
      // cleanup does not intermittently fail (notably on Windows) on a leaked handle.
      Using.resource(Files.walk(repo)) { stream =>
        stream
          .sorted(java.util.Comparator.reverseOrder[Path]())
          .forEach(p => Files.deleteIfExists(p))
      }
    }
  }

  "PhysicalFileNode" should "expose its paths, size, and empty children" in withRepo { repo =>
    val file = Files.createFile(repo.resolve("a.txt"))
    val node = new PhysicalFileNode(repo, file, 42L)
    assert(node.getAbsolutePath == file)
    assert(node.getRelativePath == repo.relativize(file))
    assert(node.getRelativePath.toString == "a.txt")
    assert(node.getSize == 42L)
    assert(node.getChildren.isEmpty)
  }

  it should "report isFile / isDirectory from the filesystem" in withRepo { repo =>
    val file = Files.createFile(repo.resolve("a.txt"))
    val dir = Files.createDirectory(repo.resolve("sub"))
    val fileNode = new PhysicalFileNode(repo, file, 0L)
    val dirNode = new PhysicalFileNode(repo, dir, 0L)
    assert(fileNode.isFile && !fileNode.isDirectory)
    assert(dirNode.isDirectory && !dirNode.isFile)
    val missing = new PhysicalFileNode(repo, repo.resolve("nope"), 0L)
    assert(!missing.isFile && !missing.isDirectory)
  }

  it should "accept a direct child and reject a non-direct one" in withRepo { repo =>
    val sub = Files.createDirectory(repo.resolve("sub"))
    val parent = new PhysicalFileNode(repo, sub, 0L)
    val child = new PhysicalFileNode(repo, sub.resolve("child.txt"), 0L)
    parent.addChildNode(child)
    assert(parent.getChildren.size == 1)
    assert(parent.getChildren.contains(child))

    val parentOfRepo = new PhysicalFileNode(repo, repo, 0L)
    val deep = new PhysicalFileNode(repo, sub.resolve("deep.txt"), 0L)
    val ex = intercept[IllegalArgumentException](parentOfRepo.addChildNode(deep))
    assert(ex.getMessage == "Child node is not a direct subpath of the parent node")
  }

  it should "define equals and hashCode over the absolute path and children only" in withRepo {
    repo =>
      val file = Files.createFile(repo.resolve("a.txt"))
      val other = Files.createFile(repo.resolve("b.txt"))
      val node = new PhysicalFileNode(repo, file, 1L)
      assert(node.equals(node)) // identity
      assert(!node.equals(null))
      assert(!node.equals("not a node"))
      // size is excluded from equals/hashCode
      val sameByPath = new PhysicalFileNode(repo, file, 999L)
      assert(node == sameByPath)
      assert(node.hashCode == sameByPath.hashCode)
      assert(node != new PhysicalFileNode(repo, other, 1L))
  }

  "PhysicalFileNode.getAllFileRelativePaths" should "collect files recursively through directories" in withRepo {
    repo =>
      val sub = Files.createDirectory(repo.resolve("sub"))
      val topFile = Files.createFile(repo.resolve("top.txt"))
      val innerFile = Files.createFile(sub.resolve("inner.txt"))
      val topNode = new PhysicalFileNode(repo, topFile, 0L)
      val dirNode = new PhysicalFileNode(repo, sub, 0L)
      dirNode.addChildNode(new PhysicalFileNode(repo, innerFile, 0L))
      val nodes = new java.util.HashSet[PhysicalFileNode]()
      nodes.add(topNode)
      nodes.add(dirNode)
      val result = PhysicalFileNode.getAllFileRelativePaths(nodes).asScala.toSet
      assert(
        result == Set(repo.relativize(topFile).toString, repo.relativize(innerFile).toString)
      )
  }
}
