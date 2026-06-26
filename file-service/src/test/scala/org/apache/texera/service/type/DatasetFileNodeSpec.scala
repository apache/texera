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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatasetFileNodeSpec extends AnyFlatSpec with Matchers {

  private def obj(path: String, size: Long): ObjectStats =
    new ObjectStats().path(path).sizeBytes(Long.box(size))

  private def child(node: DatasetFileNode, name: String): DatasetFileNode =
    node.getChildren
      .find(_.getName == name)
      .getOrElse(fail(s"expected child '$name' under '${node.getName}'"))

  "fromLakeFSRepositoryCommittedObjects" should "root the tree at a 'datasets' prefix node" in {
    val tree = DatasetFileNode.fromLakeFSRepositoryCommittedObjects(
      Map(("bob@texera.com", "twitterDataset", "v1") -> List(obj("b.txt", 50L)))
    )

    tree should have size 1
    val datasetsNode = tree.head
    datasetsNode.getName shouldBe "datasets"
    datasetsNode.getFilePath shouldBe "/datasets"
  }

  it should "nest owner/dataset/version under the datasets node with prefixed file paths" in {
    val tree = DatasetFileNode.fromLakeFSRepositoryCommittedObjects(
      Map(
        ("bob@texera.com", "twitterDataset", "v1") ->
          List(obj("dir/a.csv", 100L), obj("b.txt", 50L))
      )
    )

    val owner = child(tree.head, "bob@texera.com")
    owner.getFilePath shouldBe "/datasets/bob@texera.com"

    val version = child(child(owner, "twitterDataset"), "v1")
    version.getFilePath shouldBe "/datasets/bob@texera.com/twitterDataset/v1"

    val topFile = child(version, "b.txt")
    topFile.getNodeType shouldBe "file"
    topFile.getSize shouldBe Some(50L)
    topFile.getFilePath shouldBe "/datasets/bob@texera.com/twitterDataset/v1/b.txt"

    val nested = child(child(version, "dir"), "a.csv")
    nested.getFilePath shouldBe "/datasets/bob@texera.com/twitterDataset/v1/dir/a.csv"
  }
}
