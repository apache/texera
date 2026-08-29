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

import org.apache.texera.amber.core.storage.model.{
  LakeFSFileDocument,
  OnVersionedFileResource,
  ReadonlyLocalFileDocument,
  VirtualDocument
}
import org.apache.texera.amber.core.storage.result.iceberg.IcebergDocument
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.nio.file.{Files, Paths}
import java.util.UUID

/**
  * Unit tests for `DocumentFactory`.
  *
  * The first group pins `createOrReuseDocument`, the create-or-reuse decision
  * behind output-port storage provisioning, using injected `exists`/`open`/`create`
  * spies -- no iceberg backend, no live region.
  *
  * The remaining groups exercise the scheme-dispatch surface of
  * `openReadonlyDocument` / `openDocument` / `createDocument` / `documentExists`:
  * the dataset / file / vfs scheme arms, the unsupported-scheme guards, and the
  * vfs create/open/exists happy paths against a local Hadoop-backed Iceberg
  * catalog installed via [[LocalHadoopIcebergCatalog]].
  */
class DocumentFactorySpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val uri = new URI("vfs:///wf/result/loop-end")
  private val schema = Schema()

  override def beforeAll(): Unit = {
    super.beforeAll()
    // "wh-test" is registered explicitly so the warehouse-scoped case resolves to
    // this local catalog instead of reaching for a live REST catalog under the
    // default (`rest`) config -- otherwise the case would pass or fail according to
    // the machine's catalog type rather than the code under test.
    LocalHadoopIcebergCatalog.ensure("wh-test")
  }

  // ---------------------------------------------------------------------------
  // createOrReuseDocument
  // ---------------------------------------------------------------------------

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

  // ---------------------------------------------------------------------------
  // openReadonlyDocument -- scheme dispatch
  // ---------------------------------------------------------------------------

  private val versionHash = "97fd4c2a755b69b7c66d322eab40b7e5c2ad5d10"

  "openReadonlyDocument" should "return a dataset-typed LakeFSFileDocument for the dataset scheme" in {
    val datasetUri = new URI(s"dataset:///repo/$versionHash/file.txt")
    val doc = DocumentFactory.openReadonlyDocument(datasetUri)
    doc shouldBe a[LakeFSFileDocument]
    doc.asInstanceOf[LakeFSFileDocument].resourceType shouldBe ResourceType.Dataset
    doc.getURI shouldBe datasetUri
  }

  it should "return a model-typed LakeFSFileDocument for the model scheme and parse its URI components" in {
    val modelUri = new URI(s"model:///model-1/$versionHash/weights/model.pt")
    val doc = DocumentFactory.openReadonlyDocument(modelUri)
    doc shouldBe a[LakeFSFileDocument]
    doc.asInstanceOf[LakeFSFileDocument].resourceType shouldBe ResourceType.Model
    doc.getURI shouldBe modelUri

    val resource = doc.asInstanceOf[OnVersionedFileResource]
    resource.getRepositoryName() shouldBe "model-1"
    resource.getVersionHash() shouldBe versionHash
    resource.getFileRelativePath() shouldBe Paths.get("weights", "model.pt").toString
  }

  it should "return a ReadonlyLocalFileDocument for the file scheme" in {
    val tempFile = Files.createTempFile("doc-factory-spec", ".txt")
    try {
      val doc = DocumentFactory.openReadonlyDocument(tempFile.toUri)
      doc shouldBe a[ReadonlyLocalFileDocument]
      doc.getURI shouldBe tempFile.toUri
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  it should "reject an unsupported scheme with a descriptive message" in {
    val thrown = intercept[UnsupportedOperationException] {
      DocumentFactory.openReadonlyDocument(new URI("ftp://host/path"))
    }
    thrown.getMessage should include("ftp")
    thrown.getMessage should include("ReadonlyDocument")
  }

  // ---------------------------------------------------------------------------
  // openDocument / createDocument / documentExists -- unsupported schemes
  // ---------------------------------------------------------------------------

  "openDocument" should "return a dataset-typed LakeFSFileDocument and no schema for the dataset scheme" in {
    val datasetUri = new URI(s"dataset:///repo/$versionHash/file.txt")
    val (doc, schemaOpt) = DocumentFactory.openDocument(datasetUri)
    doc shouldBe a[LakeFSFileDocument]
    doc.asInstanceOf[LakeFSFileDocument].resourceType shouldBe ResourceType.Dataset
    schemaOpt shouldBe None
  }

  it should "return a model-typed LakeFSFileDocument and no schema for the model scheme" in {
    val modelUri = new URI(s"model:///model-1/$versionHash/weights/model.pt")
    val (doc, schemaOpt) = DocumentFactory.openDocument(modelUri)
    doc shouldBe a[LakeFSFileDocument]
    doc.asInstanceOf[LakeFSFileDocument].resourceType shouldBe ResourceType.Model
    schemaOpt shouldBe None
  }

  it should "reject an unsupported scheme" in {
    val thrown = intercept[UnsupportedOperationException] {
      DocumentFactory.openDocument(new URI("ftp://host/path"))
    }
    thrown.getMessage should include("opening the document")
  }

  "createDocument" should "reject an unsupported scheme" in {
    val thrown = intercept[UnsupportedOperationException] {
      DocumentFactory.createDocument(
        new URI("file:///tmp/x"),
        Schema().add("v", AttributeType.INTEGER)
      )
    }
    thrown.getMessage should include("creating the document")
  }

  "documentExists" should "reject an unsupported scheme" in {
    val thrown = intercept[UnsupportedOperationException] {
      DocumentFactory.documentExists(new URI("file:///tmp/x"))
    }
    thrown.getMessage should include("checking document existence")
  }

  // ---------------------------------------------------------------------------
  // vfs scheme -- create / open / exists against a local Iceberg catalog
  // ---------------------------------------------------------------------------

  private val vfsSchema: Schema = Schema().add("v", AttributeType.INTEGER)

  private def freshResultURI(): URI = {
    val base = VFSURIFactory.createPortBaseURI(
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      GlobalPortIdentity(
        PhysicalOpIdentity(
          logicalOpId = OperatorIdentity(s"op-${UUID.randomUUID().toString.replace("-", "")}"),
          layerName = "main"
        ),
        PortIdentity()
      )
    )
    VFSURIFactory.resultURI(base)
  }

  "createDocument" should "create an IcebergDocument for a vfs result URI" in {
    val vfsUri = freshResultURI()
    val doc = DocumentFactory.createDocument(vfsUri, vfsSchema)
    doc shouldBe an[IcebergDocument[_]]
    DocumentFactory.documentExists(vfsUri) shouldBe true
  }

  it should "create a state document for a vfs state URI (STATE namespace arm)" in {
    val base = VFSURIFactory.createPortBaseURI(
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      GlobalPortIdentity(
        PhysicalOpIdentity(
          logicalOpId = OperatorIdentity(s"op-${UUID.randomUUID().toString.replace("-", "")}"),
          layerName = "main"
        ),
        PortIdentity()
      )
    )
    val stateUri = VFSURIFactory.stateURI(base)
    val doc = DocumentFactory.createDocument(stateUri, vfsSchema)
    doc shouldBe an[IcebergDocument[_]]
    DocumentFactory.documentExists(stateUri) shouldBe true
  }

  it should "create + find a document for a warehouse-scoped vfs URI (the /wh/ segment is stripped from the storage key)" in {
    val base = VFSURIFactory.createPortBaseURI(
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      GlobalPortIdentity(
        PhysicalOpIdentity(
          logicalOpId = OperatorIdentity(s"op-${UUID.randomUUID().toString.replace("-", "")}"),
          layerName = "main"
        ),
        PortIdentity()
      ),
      warehouse = Some("wh-test")
    )
    val vfsUri = VFSURIFactory.resultURI(base)
    vfsUri.getPath should startWith("/wh/wh-test/")

    val doc = DocumentFactory.createDocument(vfsUri, vfsSchema)
    doc shouldBe an[IcebergDocument[_]]
    DocumentFactory.documentExists(vfsUri) shouldBe true
  }

  "documentExists" should "report false before creation and true after for a vfs URI" in {
    val vfsUri = freshResultURI()
    DocumentFactory.documentExists(vfsUri) shouldBe false
    DocumentFactory.createDocument(vfsUri, vfsSchema)
    DocumentFactory.documentExists(vfsUri) shouldBe true
  }

  it should "resolve the CONSOLE_MESSAGES namespace" in {
    val consoleUri = VFSURIFactory.createConsoleMessagesURI(
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OperatorIdentity(s"op-${UUID.randomUUID().toString.replace("-", "")}")
    )
    DocumentFactory.documentExists(consoleUri) shouldBe false
  }

  it should "resolve the RUNTIME_STATISTICS namespace" in {
    val statsUri = VFSURIFactory.createRuntimeStatisticsURI(
      WorkflowIdentity(0),
      ExecutionIdentity(0)
    )
    DocumentFactory.documentExists(statsUri) shouldBe false
  }

  "openDocument" should "open a created vfs document and return its schema" in {
    val vfsUri = freshResultURI()
    DocumentFactory.createDocument(vfsUri, vfsSchema)
    val (doc, schemaOpt) = DocumentFactory.openDocument(vfsUri)
    doc shouldBe an[IcebergDocument[_]]
    schemaOpt shouldBe defined
    schemaOpt.get.getAttributes.map(_.getName) should contain("v")
  }

  it should "throw IllegalArgumentException when opening a vfs URI with no backing storage" in {
    val thrown = intercept[IllegalArgumentException] {
      DocumentFactory.openDocument(freshResultURI())
    }
    thrown.getMessage should include("No storage is found")
  }
}
