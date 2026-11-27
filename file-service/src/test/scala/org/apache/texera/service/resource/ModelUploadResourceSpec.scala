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
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.service.`type`.LakeFSFileNode

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.Optional

class ModelUploadResourceSpec extends ModelResourceTestSupport("model_upload_user") {

  /** A second account that is granted WRITE on a model but never owns one. */
  private val collaboratorUser: User = mkUser("model_upload_collaborator", UserRoleEnum.REGULAR)
  override protected def extraUsers: Seq[User] = Seq(collaboratorUser)

  lazy val collaboratorSession = new SessionUser(collaboratorUser)

  // ---------- helpers ----------
  /** Creates a fresh model (provisions its LakeFS repo) and returns it. */
  private def newModel(): ModelResource.DashboardModel =
    createModelFixture(uniqueName("upload-model"), "for upload tests")

  /** Runs multipart-upload operation `op` on file `filePath` of `model` as the owner. */
  private def multipartOp(
      op: String,
      model: ModelResource.DashboardModel,
      filePath: String,
      fileSizeBytes: Optional[java.lang.Long] = Optional.empty(),
      partSizeBytes: Optional[java.lang.Long] = Optional.empty()
  ): Response =
    modelResource.multipartUpload(
      op,
      ownerUser.getEmail,
      model.model.getName,
      urlEnc(filePath),
      fileSizeBytes,
      partSizeBytes,
      Optional.empty(),
      sessionUser
    )

  // ===========================================================================
  // One-shot upload + version lifecycle
  // ===========================================================================
  "uploadOneFileToModel + createModelVersion" should "commit an uploaded .pt file into a version" in {
    val model = newModel()
    val mid = model.model.getMid

    upload(mid, "model.pt", Array.fill[Byte](2048)(0x5a))

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
    upload(model.model.getMid, "weights.pth", Array.fill[Byte](1024)(0x1))
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
    upload(mid, "scratch.pt", Array.fill[Byte](512)(0x2))
    modelResource.deleteModelFile(mid, urlEnc("scratch.pt"), sessionUser).getStatus shouldEqual 200
  }

  // ===========================================================================
  // No per-file type restriction: a model is a folder of files
  // ===========================================================================
  "uploadOneFileToModel" should "accept companion files alongside weights and commit them together" in {
    val model = newModel()
    val mid = model.model.getMid

    // a typical model folder: weights plus config/tokenizer companions
    upload(mid, "model.pt", Array.fill[Byte](256)(0x5))
    upload(mid, "config.json", "{\"hidden\":8}".getBytes(StandardCharsets.UTF_8))
    upload(mid, "tokenizer.txt", Array.fill[Byte](32)(0x4))

    val version = modelResource.createModelVersion("folder", mid, sessionUser)
    version.fileNodes.nonEmpty shouldBe true

    val names = modelResource.retrieveLatestModelVersion(mid, sessionUser).fileNodes.map(_.getName)
    names should contain allOf ("model.pt", "config.json", "tokenizer.txt")
  }

  it should "preserve a nested folder structure in the committed version tree" in {
    val model = newModel()
    val mid = model.model.getMid

    // a HuggingFace-style layout: files inside subdirectories
    upload(mid, "pytorch_model.bin", Array.fill[Byte](128)(0x6))
    upload(mid, "tokenizer/vocab.txt", Array.fill[Byte](64)(0x7))
    upload(mid, "shards/part-00001/data.bin", Array.fill[Byte](64)(0x8))

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
    upload(mid, "a.pt", Array.fill[Byte](100)(0x1))
    upload(mid, "b.pt", Array.fill[Byte](200)(0x2))
    upload(mid, "c.pt", Array.fill[Byte](300)(0x3))
    upload(mid, "d.pt", Array.fill[Byte](400)(0x4))
    val v1 = modelResource.createModelVersion("first", mid, sessionUser)

    // v2: re-upload ONLY b, with a different size so the two revisions are distinguishable
    upload(mid, "b.pt", Array.fill[Byte](999)(0x9))
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
    grantAccess(mid, collaboratorUser.getUid, PrivilegeEnum.WRITE)

    upload(mid, "weights.bin", Array.fill[Byte](64)(0x3), collaboratorSession)

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

    upload(mid, "weights.pt", Array.fill[Byte](32)(0x1))

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

  it should "reject a description containing a slash without committing to LakeFS" in {
    val model = newModel()
    val mid = model.model.getMid

    upload(mid, "weights.pt", Array.fill[Byte](32)(0x1))

    intercept[BadRequestException] {
      modelResource.createModelVersion("2024/01 snapshot", mid, sessionUser)
    }

    // The staged change survived, so a retry with a usable description works.
    val recovered = modelResource.createModelVersion("2024-01 snapshot", mid, sessionUser)
    recovered.modelVersion.getName should endWith("2024-01 snapshot")
  }

  "retrieveModelVersionRootFileNodes" should "404 for a version belonging to another model" in {
    val modelA = newModel()
    val modelB = newModel()

    upload(modelA.model.getMid, "a.pt", Array.fill[Byte](16)(0x1))
    upload(modelB.model.getMid, "b.pt", Array.fill[Byte](16)(0x2))
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

    // Absent means null, which used to NPE into a 500.
    for (op <- Seq(null, "", "bogus")) {
      intercept[BadRequestException] {
        multipartOp(op, model, "f.pt")
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
    val initResp = multipartOp(
      "init",
      model,
      filePath,
      Optional.of(java.lang.Long.valueOf(payload.length.toLong)),
      Optional.of(java.lang.Long.valueOf(partSize))
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
    multipartOp("finish", model, filePath).getStatus shouldEqual 200

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
    val filePath = "abort-model.pt"

    multipartOp(
      "init",
      model,
      filePath,
      Optional.of(java.lang.Long.valueOf(16L)),
      Optional.of(java.lang.Long.valueOf(8L * 1024L * 1024L))
    ).getStatus shouldEqual 200

    multipartOp("abort", model, filePath).getStatus shouldEqual 200
  }
}
