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

package org.apache.texera.service.resource

import jakarta.ws.rs._
import jakarta.ws.rs.core._
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{ModelUserAccessDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{ModelUserAccess, User}
import org.apache.texera.service.MockLakeFS
import org.apache.texera.service.`type`.LakeFSFileNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.Optional

class ModelUploadResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with MockLakeFS
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with ResourceTestHelpers {

  private val ownerUser: User = {
    val user = new User
    user.setName("model_upload_user")
    user.setEmail("model_upload_user@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  /** A second account that is granted WRITE on a model but never owns one. */
  private val collaboratorUser: User = {
    val user = new User
    user.setName("model_upload_collaborator")
    user.setEmail("model_upload_collaborator@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  lazy val modelResource = new ModelResource()
  lazy val sessionUser = new SessionUser(ownerUser)
  lazy val collaboratorSession = new SessionUser(collaboratorUser)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(ownerUser)
    userDao.insert(collaboratorUser)
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  // ---------- helpers ----------
  /** Creates a fresh model (provisions its LakeFS repo) and returns it. */
  private def newModel(): ModelResource.DashboardModel =
    modelResource.createModel(
      ModelResource.CreateModelRequest(
        modelName = uniqueName("upload-model"),
        modelDescription = "for upload tests",
        isModelPublic = false,
        isModelDownloadable = true,
        framework = "pytorch",
        format = null
      ),
      sessionUser
    )

  private def uploadOneShot(mid: Integer, path: String, bytes: Array[Byte]): Response =
    modelResource.uploadOneFileToModel(
      mid,
      urlEnc(path),
      "upload",
      new ByteArrayInputStream(bytes),
      mkHeaders(bytes.length.toLong),
      sessionUser
    )

  // ===========================================================================
  // One-shot upload + version lifecycle
  // ===========================================================================
  "uploadOneFileToModel + createModelVersion" should "commit an uploaded .pt file into a version" in {
    val model = newModel()
    val mid = model.model.getMid

    uploadOneShot(mid, "model.pt", Array.fill[Byte](2048)(0x5a)).getStatus shouldEqual 200

    val version = modelResource.createModelVersion("initial", mid, sessionUser)
    version.modelVersion.getName should startWith("v1")

    modelResource.getModelVersionList(mid, sessionUser) should have size 1

    val latest = modelResource.retrieveLatestModelVersion(mid, sessionUser)
    latest.fileNodes.map(_.getName) should contain("model.pt")

    val roots =
      modelResource.retrieveModelVersionRootFileNodes(
        mid,
        version.modelVersion.getMvid,
        sessionUser
      )
    roots.fileNodes.map(_.getName) should contain("model.pt")
    roots.size should be > 0L

    // The serialized path must carry the "model" resource-type prefix: FileResolver
    // keys on that first segment to pick the backing table, so a "/dataset/..." path
    // here would resolve a model against the dataset table.
    roots.fileNodes
      .find(_.getName == "model.pt")
      .get
      .getFilePath shouldBe s"/model/${ownerUser.getEmail}/${model.model.getName}/${version.modelVersion.getName}/model.pt"
  }

  it should "accept a .pth extension as well" in {
    val model = newModel()
    uploadOneShot(
      model.model.getMid,
      "weights.pth",
      Array.fill[Byte](1024)(0x1)
    ).getStatus shouldEqual 200
  }

  "createModelVersion" should "reject a version when there are no staged changes" in {
    val model = newModel()
    val ex = intercept[WebApplicationException] {
      modelResource.createModelVersion("empty", model.model.getMid, sessionUser)
    }
    ex.getResponse.getStatus shouldEqual 400
  }

  "deleteModelFile" should "remove a staged file" in {
    val model = newModel()
    val mid = model.model.getMid
    uploadOneShot(mid, "scratch.pt", Array.fill[Byte](512)(0x2)).getStatus shouldEqual 200
    modelResource.deleteModelFile(mid, urlEnc("scratch.pt"), sessionUser).getStatus shouldEqual 200
  }

  // ===========================================================================
  // No per-file type restriction: a model is a folder of files
  // ===========================================================================
  "uploadOneFileToModel" should "accept companion files alongside weights and commit them together" in {
    val model = newModel()
    val mid = model.model.getMid

    // a typical model folder: weights plus config/tokenizer companions
    uploadOneShot(mid, "model.pt", Array.fill[Byte](256)(0x5)).getStatus shouldEqual 200
    uploadOneShot(
      mid,
      "config.json",
      "{\"hidden\":8}".getBytes(StandardCharsets.UTF_8)
    ).getStatus shouldEqual 200
    uploadOneShot(mid, "tokenizer.txt", Array.fill[Byte](32)(0x4)).getStatus shouldEqual 200

    val version = modelResource.createModelVersion("folder", mid, sessionUser)
    version.fileNodes.nonEmpty shouldBe true

    val names = modelResource.retrieveLatestModelVersion(mid, sessionUser).fileNodes.map(_.getName)
    names should contain allOf ("model.pt", "config.json", "tokenizer.txt")
  }

  it should "preserve a nested folder structure in the committed version tree" in {
    val model = newModel()
    val mid = model.model.getMid

    // a HuggingFace-style layout: files inside subdirectories
    uploadOneShot(mid, "pytorch_model.bin", Array.fill[Byte](128)(0x6)).getStatus shouldEqual 200
    uploadOneShot(
      mid,
      "tokenizer/vocab.txt",
      Array.fill[Byte](64)(0x7)
    ).getStatus shouldEqual 200
    uploadOneShot(
      mid,
      "shards/part-00001/data.bin",
      Array.fill[Byte](64)(0x8)
    ).getStatus shouldEqual 200

    modelResource.createModelVersion("nested", mid, sessionUser)

    val roots = modelResource.retrieveLatestModelVersion(mid, sessionUser).fileNodes
    roots.map(_.getName) should contain allOf ("pytorch_model.bin", "tokenizer", "shards")

    // directories are preserved as directory nodes holding their children
    val tokenizerDir = roots.find(_.getName == "tokenizer").get
    tokenizerDir.getNodeType shouldEqual "directory"
    tokenizerDir.getChildren.map(_.getName) should contain("vocab.txt")

    // nesting is recursive, not flattened to one level
    val shardsDir = roots.find(_.getName == "shards").get
    val partDir = shardsDir.getChildren.find(_.getName == "part-00001").get
    partDir.getNodeType shouldEqual "directory"
    partDir.getChildren.map(_.getName) should contain("data.bin")
  }

  // ===========================================================================
  // Version semantics: each version is a full snapshot, not a delta
  // ===========================================================================
  "a later version" should "carry over untouched files and only replace the re-uploaded one" in {
    val model = newModel()
    val mid = model.model.getMid

    // v1: four files, with b at a known size
    uploadOneShot(mid, "a.pt", Array.fill[Byte](100)(0x1)).getStatus shouldEqual 200
    uploadOneShot(mid, "b.pt", Array.fill[Byte](200)(0x2)).getStatus shouldEqual 200
    uploadOneShot(mid, "c.pt", Array.fill[Byte](300)(0x3)).getStatus shouldEqual 200
    uploadOneShot(mid, "d.pt", Array.fill[Byte](400)(0x4)).getStatus shouldEqual 200
    val v1 = modelResource.createModelVersion("first", mid, sessionUser)

    // v2: re-upload ONLY b, with a different size so the two revisions are distinguishable
    uploadOneShot(mid, "b.pt", Array.fill[Byte](999)(0x9)).getStatus shouldEqual 200
    val v2 = modelResource.createModelVersion("second", mid, sessionUser)

    def nodesOf(mvid: Integer) =
      modelResource.retrieveModelVersionRootFileNodes(mid, mvid, sessionUser).fileNodes
    def sizeOf(mvid: Integer, name: String) =
      nodesOf(mvid).find(_.getName == name).flatMap(_.getSize)

    // v2 still contains all four files: a, c, d carried over untouched, b replaced
    nodesOf(v2.modelVersion.getMvid)
      .map(_.getName) should contain allOf ("a.pt", "b.pt", "c.pt", "d.pt")
    sizeOf(v2.modelVersion.getMvid, "a.pt") shouldEqual Some(100L)
    sizeOf(v2.modelVersion.getMvid, "c.pt") shouldEqual Some(300L)
    sizeOf(v2.modelVersion.getMvid, "d.pt") shouldEqual Some(400L)
    sizeOf(v2.modelVersion.getMvid, "b.pt") shouldEqual Some(999L)

    // v1 is immutable: it still sees the ORIGINAL b
    sizeOf(v1.modelVersion.getMvid, "b.pt") shouldEqual Some(200L)

    // both versions are listed, newest first
    modelResource.getModelVersionList(mid, sessionUser).map(_.getName) should have size 2
  }

  // ===========================================================================
  // Path ownership
  // ===========================================================================
  "a version created by a WRITE collaborator" should "carry the owner's email in its file paths" in {
    val model = newModel()
    val mid = model.model.getMid

    // The collaborator can write, but the model still belongs to ownerUser.
    new ModelUserAccessDao(getDSLContext.configuration())
      .insert(new ModelUserAccess(mid, collaboratorUser.getUid, PrivilegeEnum.WRITE))

    modelResource
      .uploadOneFileToModel(
        mid,
        urlEnc("weights.bin"),
        "upload",
        new ByteArrayInputStream(Array.fill[Byte](64)(0x3)),
        mkHeaders(64L),
        collaboratorSession
      )
      .getStatus shouldEqual 200

    val created = modelResource.createModelVersion("from-collab", mid, collaboratorSession)
    val versionName = created.modelVersion.getName

    // FileResolver resolves /model/<ownerEmail>/... via MODEL.OWNER_UID, so a path naming
    // the collaborator resolves to nothing.
    val expected =
      s"/model/${ownerUser.getEmail}/${model.model.getName}/$versionName/weights.bin"

    def pathsOf(nodes: List[LakeFSFileNode]): List[String] =
      nodes.flatMap(n => n.getFilePath :: pathsOf(n.getChildren))

    pathsOf(created.fileNodes) should contain(expected)
    pathsOf(created.fileNodes).foreach(_ should not include collaboratorUser.getEmail)

    // The response must agree with a subsequent read.
    pathsOf(
      modelResource.retrieveLatestModelVersion(mid, collaboratorSession).fileNodes
    ) should contain(expected)
  }

  // ===========================================================================
  // Input validation and version scoping
  // ===========================================================================
  "createModelVersion" should "reject an over-long name without committing to LakeFS" in {
    val model = newModel()
    val mid = model.model.getMid

    uploadOneShot(mid, "weights.pt", Array.fill[Byte](32)(0x1)).getStatus shouldEqual 200

    // The insert happens after the LakeFS commit, so an unchecked name would strand the
    // staged file behind a commit no version points at.
    val tooLong = "x" * 200
    val thrown = intercept[BadRequestException] {
      modelResource.createModelVersion(tooLong, mid, sessionUser)
    }
    thrown.getMessage should include("too long")

    // The staged change survived, so a retry works -- previously it hit "No changes detected".
    val recovered = modelResource.createModelVersion("sane-name", mid, sessionUser)
    recovered.modelVersion.getName should endWith("sane-name")
    modelResource
      .retrieveLatestModelVersion(mid, sessionUser)
      .fileNodes
      .map(_.getName) should contain("weights.pt")
  }

  "retrieveModelVersionRootFileNodes" should "404 for a version belonging to another model" in {
    val modelA = newModel()
    val modelB = newModel()

    uploadOneShot(modelA.model.getMid, "a.pt", Array.fill[Byte](16)(0x1)).getStatus shouldEqual 200
    uploadOneShot(modelB.model.getMid, "b.pt", Array.fill[Byte](16)(0x2)).getStatus shouldEqual 200
    modelResource.createModelVersion("va", modelA.model.getMid, sessionUser)
    val versionOfB = modelResource.createModelVersion("vb", modelB.model.getMid, sessionUser)

    // An unscoped lookup would resolve B's version through A's repository and 500 in LakeFS.
    intercept[NotFoundException] {
      modelResource.retrieveModelVersionRootFileNodes(
        modelA.model.getMid,
        versionOfB.modelVersion.getMvid,
        sessionUser
      )
    }
  }

  "multipartUpload" should "400 when the operation type is missing or unknown" in {
    val model = newModel()
    val ownerEmail = ownerUser.getEmail
    val modelName = model.model.getName

    // Absent means null, which used to NPE into a 500.
    for (op <- Seq(null, "", "bogus")) {
      intercept[BadRequestException] {
        modelResource.multipartUpload(
          op,
          ownerEmail,
          modelName,
          urlEnc("f.pt"),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          sessionUser
        )
      }
    }
  }

  // ===========================================================================
  // Session-based multipart upload (single part)
  // ===========================================================================
  "the multipart flow" should "init, upload a part, finish, and be committable as a version" in {
    val model = newModel()
    val mid = model.model.getMid
    val ownerEmail = ownerUser.getEmail
    val modelName = model.model.getName
    val filePath = "multipart-model.pt"
    val payload = Array.fill[Byte](16)(0x7)
    val partSize = 8L * 1024L * 1024L

    // init -> one part expected
    val initResp = modelResource.multipartUpload(
      "init",
      ownerEmail,
      modelName,
      urlEnc(filePath),
      Optional.of(java.lang.Long.valueOf(payload.length.toLong)),
      Optional.of(java.lang.Long.valueOf(partSize)),
      Optional.empty(),
      sessionUser
    )
    initResp.getStatus shouldEqual 200

    // upload the single part
    val partResp = modelResource.uploadPart(
      ownerEmail,
      modelName,
      urlEnc(filePath),
      1,
      new ByteArrayInputStream(payload),
      mkHeaders(payload.length.toLong),
      sessionUser
    )
    partResp.getStatus shouldEqual 200

    // finish
    val finishResp = modelResource.multipartUpload(
      "finish",
      ownerEmail,
      modelName,
      urlEnc(filePath),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      sessionUser
    )
    finishResp.getStatus shouldEqual 200

    // the finished file is now staged and can be committed as a version
    val version = modelResource.createModelVersion("from-multipart", mid, sessionUser)
    version.fileNodes.nonEmpty shouldBe true
    modelResource
      .retrieveLatestModelVersion(mid, sessionUser)
      .fileNodes
      .map(_.getName) should contain(filePath)
  }

  it should "abort an initiated upload" in {
    val model = newModel()
    val ownerEmail = ownerUser.getEmail
    val modelName = model.model.getName
    val filePath = "abort-model.pt"

    modelResource
      .multipartUpload(
        "init",
        ownerEmail,
        modelName,
        urlEnc(filePath),
        Optional.of(java.lang.Long.valueOf(16L)),
        Optional.of(java.lang.Long.valueOf(8L * 1024L * 1024L)),
        Optional.empty(),
        sessionUser
      )
      .getStatus shouldEqual 200

    modelResource
      .multipartUpload(
        "abort",
        ownerEmail,
        modelName,
        urlEnc(filePath),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        sessionUser
      )
      .getStatus shouldEqual 200
  }
}
