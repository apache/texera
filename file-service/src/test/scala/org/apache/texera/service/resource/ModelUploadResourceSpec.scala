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
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.service.MockLakeFS
import org.apache.texera.service.`type`.{ExistingUploadFile, ExistingUploadFilesRequest}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.ByteArrayInputStream
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.{Collections, Date, Locale, Optional}
import scala.jdk.CollectionConverters._
import scala.util.Random

class ModelUploadResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with MockLakeFS
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val ownerUser: User = {
    val user = new User
    user.setName("model_upload_user")
    user.setPassword("123")
    user.setEmail("model_upload_user@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  private val strangerUser: User = {
    val user = new User
    user.setName("model_upload_stranger")
    user.setPassword("123")
    user.setEmail("model_upload_stranger@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  lazy val modelResource = new ModelResource()
  lazy val sessionUser = new SessionUser(ownerUser)
  lazy val strangerSession = new SessionUser(strangerUser)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(ownerUser)
    userDao.insert(strangerUser)
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  // ---------- helpers ----------
  private def urlEnc(raw: String): String =
    URLEncoder.encode(raw, StandardCharsets.UTF_8.name())

  private def uniqueName(prefix: String): String =
    s"$prefix-${System.nanoTime()}-${Random.alphanumeric.take(6).mkString.toLowerCase}"

  /** Minimal HttpHeaders exposing only Content-Length, which the upload paths read. */
  private def mkHeaders(contentLength: Long): HttpHeaders =
    new HttpHeaders {
      private val headers = new MultivaluedHashMap[String, String]()
      headers.putSingle(HttpHeaders.CONTENT_LENGTH, contentLength.toString)
      override def getHeaderString(name: String): String = headers.getFirst(name)
      override def getRequestHeaders: MultivaluedMap[String, String] = headers
      override def getRequestHeader(name: String): java.util.List[String] =
        Option(headers.get(name)).getOrElse(Collections.emptyList[String]())
      override def getAcceptableMediaTypes: java.util.List[MediaType] = Collections.emptyList()
      override def getAcceptableLanguages: java.util.List[Locale] = Collections.emptyList()
      override def getMediaType: MediaType = null
      override def getLanguage: Locale = null
      override def getCookies: java.util.Map[String, Cookie] = Collections.emptyMap()
      override def getDate: Date = null
      override def getLength: Int = contentLength.toInt
    }

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

  /** Reads the `filePaths` list out of an existing-upload-files response. */
  private def filePathsOf(response: Response): List[String] =
    response.getEntity match {
      case m: scala.collection.Map[_, _] =>
        m.asInstanceOf[scala.collection.Map[String, Any]]("filePaths") match {
          case l: java.util.List[_]       => l.asScala.map(_.toString).toList
          case l: scala.collection.Seq[_] => l.map(_.toString).toList
          case other                      => fail(s"Expected a list, got: ${other.getClass}")
        }
      case other => fail(s"Unexpected response entity type: ${other.getClass}")
    }

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

  // ===========================================================================
  // Staged changes (diff) — what the upload UI shows before a version is cut
  // ===========================================================================
  "getModelDiff" should "list an uploaded file as a staged change" in {
    val model = newModel()
    val mid = model.model.getMid
    uploadOneShot(mid, "staged.pt", Array.fill[Byte](256)(0x9)).getStatus shouldEqual 200

    val diffs = modelResource.getModelDiff(mid, sessionUser)
    diffs.map(_.path) should contain("staged.pt")
    diffs.find(_.path == "staged.pt").flatMap(_.sizeBytes) shouldBe Some(256L)
  }

  it should "report nothing for a model with no staged changes" in {
    modelResource.getModelDiff(newModel().model.getMid, sessionUser) shouldBe empty
  }

  it should "stop listing a file once it has been committed into a version" in {
    val model = newModel()
    val mid = model.model.getMid
    uploadOneShot(mid, "committed.pt", Array.fill[Byte](128)(0x3)).getStatus shouldEqual 200
    modelResource.createModelVersion("v1", mid, sessionUser)

    modelResource.getModelDiff(mid, sessionUser) shouldBe empty
  }

  it should "refuse a caller with no access to the model" in {
    val model = newModel()
    assertThrows[ForbiddenException] {
      modelResource.getModelDiff(model.model.getMid, strangerSession)
    }
  }

  "resetModelFileDiff" should "discard a staged file" in {
    val model = newModel()
    val mid = model.model.getMid
    uploadOneShot(mid, "discard-me.pt", Array.fill[Byte](64)(0x4)).getStatus shouldEqual 200

    modelResource
      .resetModelFileDiff(mid, urlEnc("discard-me.pt"), sessionUser)
      .getStatus shouldEqual 200

    modelResource.getModelDiff(mid, sessionUser).map(_.path) should not contain "discard-me.pt"
  }

  it should "decode a url-encoded nested path" in {
    val model = newModel()
    val mid = model.model.getMid
    val nested = "tokenizer/vocab file.txt"
    uploadOneShot(mid, nested, Array.fill[Byte](32)(0x5)).getStatus shouldEqual 200

    modelResource.resetModelFileDiff(mid, urlEnc(nested), sessionUser).getStatus shouldEqual 200

    modelResource.getModelDiff(mid, sessionUser).map(_.path) should not contain nested
  }

  it should "refuse a caller without write access" in {
    val model = newModel()
    assertThrows[ForbiddenException] {
      modelResource.resetModelFileDiff(model.model.getMid, urlEnc("x.pt"), strangerSession)
    }
  }

  // ===========================================================================
  // existing-upload-files — lets the UI skip re-uploading identical files
  // ===========================================================================
  "findExistingUploadFiles" should "match committed and staged files by path and size" in {
    val model = newModel()
    val mid = model.model.getMid
    val committed = Array.fill[Byte](200)(0x1)
    val staged = Array.fill[Byte](300)(0x2)

    uploadOneShot(mid, "committed.pt", committed).getStatus shouldEqual 200
    modelResource.createModelVersion("v1", mid, sessionUser)
    uploadOneShot(mid, "staged.pt", staged).getStatus shouldEqual 200

    val response = modelResource.findExistingUploadFiles(
      mid,
      ExistingUploadFilesRequest(
        List(
          ExistingUploadFile("committed.pt", committed.length.toLong),
          ExistingUploadFile("staged.pt", staged.length.toLong),
          ExistingUploadFile("committed.pt", committed.length + 1L), // right path, wrong size
          ExistingUploadFile("absent.pt", 1L)
        )
      ),
      sessionUser
    )

    response.getStatus shouldEqual 200
    filePathsOf(response) shouldEqual List("committed.pt", "staged.pt")
  }

  it should "tolerate a null file list" in {
    val response = modelResource.findExistingUploadFiles(
      newModel().model.getMid,
      ExistingUploadFilesRequest(null),
      sessionUser
    )
    response.getStatus shouldEqual 200
    filePathsOf(response) shouldBe empty
  }

  it should "reject a path that escapes the repository root" in {
    assertThrows[BadRequestException] {
      modelResource.findExistingUploadFiles(
        newModel().model.getMid,
        ExistingUploadFilesRequest(List(ExistingUploadFile("../escape.pt", 1L))),
        sessionUser
      )
    }
  }

  it should "refuse a caller without write access" in {
    val model = newModel()
    assertThrows[ForbiddenException] {
      modelResource.findExistingUploadFiles(
        model.model.getMid,
        ExistingUploadFilesRequest(List(ExistingUploadFile("a.pt", 1L))),
        strangerSession
      )
    }
  }

  // ===========================================================================
  // listModels sizes
  // ===========================================================================
  "listModels" should "report the repository size of an owned model" in {
    val model = newModel()
    val mid = model.model.getMid
    uploadOneShot(mid, "sized.pt", Array.fill[Byte](4096)(0x6)).getStatus shouldEqual 200
    modelResource.createModelVersion("v1", mid, sessionUser)

    val listed = modelResource
      .listModels(sessionUser)
      .find(_.model.getMid == mid)
      .getOrElse(fail("the owned model should be listed"))

    listed.isOwner shouldBe true
    listed.size should be >= 4096L
  }

  it should "still list a model whose repository size cannot be read" in {
    // An unreadable size degrades to 0 rather than dropping the row.
    val model = newModel()
    val mid = model.model.getMid
    LakeFSStorageClient.deleteRepo(model.model.getRepositoryName)

    val listed = modelResource.listModels(sessionUser).find(_.model.getMid == mid)

    listed should not be empty
    listed.get.size shouldEqual 0L
  }
}
