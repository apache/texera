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

import org.apache.texera.amber.core.storage.FileResolver.{
  DATASET_FILE_URI_SCHEME,
  MODEL_FILE_URI_SCHEME
}
import org.apache.texera.amber.core.storage.model.{
  DatasetFileDocument,
  ModelFileDocument,
  OnVersionedFileResource,
  VirtualDocument
}
import org.apache.texera.amber.core.tuple.{Schema, Tuple}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.nio.file.Paths

/**
  * Unit tests for `DocumentFactory.createOrReuseDocument`, the create-or-reuse
  * decision behind output-port storage provisioning. It always returns the
  * document (opened when reused, created otherwise) so the call site doesn't
  * branch.
  *
  * `exists` / `open` / `create` are injected so the decision can be pinned with
  * trivial document stubs -- no iceberg backend, no live region.
  */
class DocumentFactorySpec extends AnyFlatSpec with Matchers {

  private val uri = new URI("vfs:///wf/result/loop-end")
  private val schema = Schema()

  private def stubDoc: VirtualDocument[Tuple] =
    new VirtualDocument[Tuple] {
      override def getURI: URI = uri
      override def clear(): Unit = ()
    }
  private val opened: VirtualDocument[_] = stubDoc
  private val created: VirtualDocument[_] = stubDoc

  /** Run with spies; return (document handed back, which path was taken). */
  private def run(reuseExisting: Boolean, exists: Boolean): (VirtualDocument[_], String) = {
    var path = ""
    val doc = DocumentFactory.createOrReuseDocument(
      uri,
      schema,
      reuseExisting,
      _ => exists,
      _ => { path = "open"; opened },
      (_, _) => { path = "create"; created }
    )
    (doc, path)
  }

  "createOrReuseDocument" should
    "open and return the existing document when the port reuses storage and one exists" in {
    val (doc, path) = run(reuseExisting = true, exists = true)
    path shouldBe "open"
    doc should be theSameInstanceAs opened
  }

  it should "create when the port reuses storage but none exists yet" in {
    val (doc, path) = run(reuseExisting = true, exists = false)
    path shouldBe "create"
    doc should be theSameInstanceAs created
  }

  it should "always create when the port does not reuse storage, even if one exists" in {
    val (doc, path) = run(reuseExisting = false, exists = true)
    path shouldBe "create"
    doc should be theSameInstanceAs created
  }

  it should "not probe existence when the port does not reuse storage" in {
    var probed = false
    DocumentFactory.createOrReuseDocument(
      uri,
      schema,
      reuseExisting = false,
      _ => { probed = true; true },
      _ => opened,
      (_, _) => created
    )
    probed shouldBe false
  }

  // Scheme routing: openReadonlyDocument dispatches a resolved {scheme}:///repo/hash/file URI to
  // the matching versioned-file document. Constructing these documents only parses the URI (no DB
  // or LakeFS access), so we can assert routing + parsing without any backend.
  "openReadonlyDocument" should "route the dataset scheme to a DatasetFileDocument" in {
    val doc = DocumentFactory.openReadonlyDocument(
      new URI(s"$DATASET_FILE_URI_SCHEME:///dataset-1/abc123/dir/a.csv")
    )
    doc shouldBe a[DatasetFileDocument]
  }

  it should "route the model scheme to a ModelFileDocument and parse its URI components" in {
    val doc = DocumentFactory.openReadonlyDocument(
      new URI(s"$MODEL_FILE_URI_SCHEME:///model-1/abc123/weights/model.pt")
    )
    doc shouldBe a[ModelFileDocument]

    val resource = doc.asInstanceOf[OnVersionedFileResource]
    resource.getRepositoryName() shouldBe "model-1"
    resource.getVersionHash() shouldBe "abc123"
    resource.getFileRelativePath() shouldBe Paths.get("weights", "model.pt").toString
  }

  it should "reject an unsupported scheme" in {
    an[UnsupportedOperationException] should be thrownBy
      DocumentFactory.openReadonlyDocument(new URI("bogus:///repo/hash/file"))
  }
}
