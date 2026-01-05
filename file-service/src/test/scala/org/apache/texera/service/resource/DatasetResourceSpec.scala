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

import ch.qos.logback.classic.{Level, Logger}
import io.lakefs.clients.sdk.ApiException
import jakarta.ws.rs._
import jakarta.ws.rs.core.{Cookie, HttpHeaders, MediaType, MultivaluedHashMap, Response}
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSession.DATASET_UPLOAD_SESSION
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSessionPart.DATASET_UPLOAD_SESSION_PART
import org.apache.texera.dao.jooq.generated.tables.daos.{DatasetDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, User}
import org.apache.texera.service.MockLakeFS
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.scalatest.tagobjects.Slow
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Tag}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import java.util.concurrent.CyclicBarrier
import java.util.{Collections, Date, Locale, Optional}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Random

object StressMultipart extends Tag("org.apache.texera.stress.multipart")

class DatasetResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with MockLakeFS
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  // ---------- logging (multipart tests can be noisy) ----------
  private var savedLevels: Map[String, Level] = Map.empty

  private def setLoggerLevel(loggerName: String, newLevel: Level): Level = {
    val logger = LoggerFactory.getLogger(loggerName).asInstanceOf[Logger]
    val prev = logger.getLevel
    logger.setLevel(newLevel)
    prev
  }

  // ---------- execution context (multipart race tests) ----------
  private implicit val ec: ExecutionContext = ExecutionContext.global

  // ---------------------------------------------------------------------------
  // Shared fixtures (DatasetResource basic tests)
  // ---------------------------------------------------------------------------
  private val ownerUser: User = {
    val user = new User
    user.setName("test_user")
    user.setPassword("123")
    user.setEmail("test_user@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  private val otherAdminUser: User = {
    val user = new User
    user.setName("test_user2")
    user.setPassword("123")
    user.setEmail("test_user2@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  // REGULAR user used specifically for multipart "no WRITE access" tests.
  private val multipartNoWriteUser: User = {
    val user = new User
    user.setName("multipart_user2")
    user.setPassword("123")
    user.setEmail("multipart_user2@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val baseDataset: Dataset = {
    val dataset = new Dataset
    dataset.setName("test-dataset")
    dataset.setRepositoryName("test-dataset")
    dataset.setIsPublic(true)
    dataset.setIsDownloadable(true)
    dataset.setDescription("dataset for test")
    dataset
  }

  // ---------------------------------------------------------------------------
  // Multipart fixtures
  // ---------------------------------------------------------------------------
  private val multipartRepoName: String =
    s"multipart-ds-${System.nanoTime()}-${Random.alphanumeric.take(6).mkString.toLowerCase}"

  private val multipartDataset: Dataset = {
    val dataset = new Dataset
    dataset.setName("multipart-ds")
    dataset.setRepositoryName(multipartRepoName)
    dataset.setIsPublic(true)
    dataset.setIsDownloadable(true)
    dataset.setDescription("dataset for multipart upload tests")
    dataset
  }

  // ---------- DAOs / resource ----------
  lazy val datasetDao = new DatasetDao(getDSLContext.configuration())
  lazy val datasetResource = new DatasetResource()

  // ---------- session users ----------
  lazy val sessionUser = new SessionUser(ownerUser)
  lazy val sessionUser2 = new SessionUser(otherAdminUser)

  // Multipart callers
  lazy val multipartOwnerSessionUser = sessionUser
  lazy val multipartNoWriteSessionUser = new SessionUser(multipartNoWriteUser)

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------
  override protected def beforeAll(): Unit = {
    super.beforeAll()

    // init db
    initializeDBAndReplaceDSLContext()

    // insert users
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(ownerUser)
    userDao.insert(otherAdminUser)
    userDao.insert(multipartNoWriteUser)

    // insert datasets (owned by ownerUser)
    baseDataset.setOwnerUid(ownerUser.getUid)
    multipartDataset.setOwnerUid(ownerUser.getUid)

    datasetDao.insert(baseDataset)
    datasetDao.insert(multipartDataset)

    savedLevels = Map(
      "org.apache.http.wire" -> setLoggerLevel("org.apache.http.wire", Level.WARN),
      "org.apache.http.headers" -> setLoggerLevel("org.apache.http.headers", Level.WARN)
    )
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()

    // Multipart repo must exist for presigned multipart init to succeed.
    // If it already exists, ignore 409.
    try LakeFSStorageClient.initRepo(multipartDataset.getRepositoryName)
    catch {
      case e: ApiException if e.getCode == 409 => // ok
    }
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally {
      try savedLevels.foreach { case (name, prev) => setLoggerLevel(name, prev) } finally super
        .afterAll()
    }
  }

  // ===========================================================================
  // DatasetResourceSpec (original basic tests)
  // ===========================================================================
  "createDataset" should "create dataset successfully if user does not have a dataset with the same name" in {
    val createDatasetRequest = DatasetResource.CreateDatasetRequest(
      datasetName = "new-dataset",
      datasetDescription = "description for new dataset",
      isDatasetPublic = false,
      isDatasetDownloadable = true
    )

    val createdDataset = datasetResource.createDataset(createDatasetRequest, sessionUser)
    createdDataset.dataset.getName shouldEqual "new-dataset"
    createdDataset.dataset.getDescription shouldEqual "description for new dataset"
    createdDataset.dataset.getIsPublic shouldBe false
    createdDataset.dataset.getIsDownloadable shouldBe true
  }

  it should "refuse to create dataset if user already has a dataset with the same name" in {
    val createDatasetRequest = DatasetResource.CreateDatasetRequest(
      datasetName = "test-dataset",
      datasetDescription = "description for new dataset",
      isDatasetPublic = false,
      isDatasetDownloadable = true
    )

    assertThrows[BadRequestException] {
      datasetResource.createDataset(createDatasetRequest, sessionUser)
    }
  }

  it should "create dataset successfully if another user has a dataset with the same name" in {
    val createDatasetRequest = DatasetResource.CreateDatasetRequest(
      datasetName = "test-dataset",
      datasetDescription = "description for new dataset",
      isDatasetPublic = false,
      isDatasetDownloadable = true
    )

    val createdDataset = datasetResource.createDataset(createDatasetRequest, sessionUser2)
    createdDataset.dataset.getName shouldEqual "test-dataset"
    createdDataset.dataset.getDescription shouldEqual "description for new dataset"
    createdDataset.dataset.getIsPublic shouldBe false
    createdDataset.dataset.getIsDownloadable shouldBe true
  }

  it should "return DashboardDataset with correct owner email, WRITE privilege, and isOwner=true" in {
    val createDatasetRequest = DatasetResource.CreateDatasetRequest(
      datasetName = "dashboard-dataset-test",
      datasetDescription = "test for DashboardDataset properties",
      isDatasetPublic = true,
      isDatasetDownloadable = false
    )

    val dashboardDataset = datasetResource.createDataset(createDatasetRequest, sessionUser)

    dashboardDataset.ownerEmail shouldEqual ownerUser.getEmail
    dashboardDataset.accessPrivilege shouldEqual PrivilegeEnum.WRITE
    dashboardDataset.isOwner shouldBe true
    dashboardDataset.size shouldEqual 0

    dashboardDataset.dataset.getName shouldEqual "dashboard-dataset-test"
    dashboardDataset.dataset.getDescription shouldEqual "test for DashboardDataset properties"
    dashboardDataset.dataset.getIsPublic shouldBe true
    dashboardDataset.dataset.getIsDownloadable shouldBe false
  }

  it should "delete dataset successfully if user owns it" in {
    val dataset = new Dataset
    dataset.setName("delete-ds")
    dataset.setRepositoryName("delete-ds")
    dataset.setDescription("for delete test")
    dataset.setOwnerUid(ownerUser.getUid)
    dataset.setIsPublic(true)
    dataset.setIsDownloadable(true)
    datasetDao.insert(dataset)

    LakeFSStorageClient.initRepo(dataset.getRepositoryName)

    val response = datasetResource.deleteDataset(dataset.getDid, sessionUser)

    response.getStatus shouldEqual 200
    datasetDao.fetchOneByDid(dataset.getDid) shouldBe null
  }

  it should "refuse to delete dataset if not owned by user" in {
    val dataset = new Dataset
    dataset.setName("user1-ds")
    dataset.setRepositoryName("user1-ds")
    dataset.setDescription("for forbidden test")
    dataset.setOwnerUid(ownerUser.getUid)
    dataset.setIsPublic(true)
    dataset.setIsDownloadable(true)
    datasetDao.insert(dataset)

    LakeFSStorageClient.initRepo(dataset.getRepositoryName)

    assertThrows[ForbiddenException] {
      datasetResource.deleteDataset(dataset.getDid, sessionUser2)
    }

    datasetDao.fetchOneByDid(dataset.getDid) should not be null
  }

  // ===========================================================================
  // Multipart upload tests (merged in)
  // ===========================================================================

  // ---------- SHA-256 Utils ----------
  private def sha256OfChunks(chunks: Seq[Array[Byte]]): Array[Byte] = {
    val messageDigest = MessageDigest.getInstance("SHA-256")
    chunks.foreach(messageDigest.update)
    messageDigest.digest()
  }

  private def sha256OfFile(path: java.nio.file.Path): Array[Byte] = {
    val messageDigest = MessageDigest.getInstance("SHA-256")
    val inputStream = Files.newInputStream(path)
    try {
      val buffer = new Array[Byte](8192)
      var bytesRead = inputStream.read(buffer)
      while (bytesRead != -1) {
        messageDigest.update(buffer, 0, bytesRead)
        bytesRead = inputStream.read(buffer)
      }
      messageDigest.digest()
    } finally inputStream.close()
  }

  // ---------- helpers ----------
  private def urlEnc(raw: String): String =
    URLEncoder.encode(raw, StandardCharsets.UTF_8.name())

  /** Minimum part-size rule (S3-style): every part except the LAST must be >= 5 MiB. */
  private val MinNonFinalPartBytes: Int = 5 * 1024 * 1024
  private def minPartBytes(fillByte: Byte): Array[Byte] =
    Array.fill[Byte](MinNonFinalPartBytes)(fillByte)

  private def tinyBytes(fillByte: Byte, n: Int = 1): Array[Byte] =
    Array.fill[Byte](n)(fillByte)

  /** InputStream that behaves like a mid-flight network drop after N bytes. */
  private def flakyStream(
      payload: Array[Byte],
      failAfterBytes: Int,
      msg: String = "simulated network drop"
  ): InputStream =
    new InputStream {
      private var pos = 0
      override def read(): Int = {
        if (pos >= failAfterBytes) throw new IOException(msg)
        if (pos >= payload.length) return -1
        val nextByte = payload(pos) & 0xff
        pos += 1
        nextByte
      }
    }

  /** Minimal HttpHeaders impl needed by DatasetResource.uploadPart */
  private def mkHeaders(contentLength: Long): HttpHeaders =
    new HttpHeaders {
      private val headers = new MultivaluedHashMap[String, String]()
      headers.putSingle(HttpHeaders.CONTENT_LENGTH, contentLength.toString)

      override def getHeaderString(name: String): String = headers.getFirst(name)
      override def getRequestHeaders = headers
      override def getRequestHeader(name: String) =
        Option(headers.get(name)).getOrElse(Collections.emptyList[String]())

      override def getAcceptableMediaTypes = Collections.emptyList[MediaType]()
      override def getAcceptableLanguages = Collections.emptyList[Locale]()
      override def getMediaType: MediaType = null
      override def getLanguage: Locale = null
      override def getCookies = Collections.emptyMap[String, Cookie]()
      override def getDate: Date = null
      override def getLength: Int = contentLength.toInt
    }

  private def mkHeadersMissingContentLength: HttpHeaders =
    new HttpHeaders {
      private val headers = new MultivaluedHashMap[String, String]()
      override def getHeaderString(name: String): String = null
      override def getRequestHeaders = headers
      override def getRequestHeader(name: String) = Collections.emptyList[String]()
      override def getAcceptableMediaTypes = Collections.emptyList[MediaType]()
      override def getAcceptableLanguages = Collections.emptyList[Locale]()
      override def getMediaType: MediaType = null
      override def getLanguage: Locale = null
      override def getCookies = Collections.emptyMap[String, Cookie]()
      override def getDate: Date = null
      override def getLength: Int = -1
    }

  private def uniqueFilePath(prefix: String): String =
    s"$prefix/${System.nanoTime()}-${Random.alphanumeric.take(8).mkString}.bin"

  private def initUpload(
      filePath: String,
      numParts: Int,
      user: SessionUser = multipartOwnerSessionUser
  ): Response =
    datasetResource.multipartUpload(
      "init",
      ownerUser.getEmail,
      multipartDataset.getName,
      urlEnc(filePath),
      Optional.of(numParts),
      user
    )

  private def finishUpload(
      filePath: String,
      user: SessionUser = multipartOwnerSessionUser
  ): Response =
    datasetResource.multipartUpload(
      "finish",
      ownerUser.getEmail,
      multipartDataset.getName,
      urlEnc(filePath),
      Optional.empty(),
      user
    )

  private def abortUpload(
      filePath: String,
      user: SessionUser = multipartOwnerSessionUser
  ): Response =
    datasetResource.multipartUpload(
      "abort",
      ownerUser.getEmail,
      multipartDataset.getName,
      urlEnc(filePath),
      Optional.empty(),
      user
    )

  private def uploadPart(
      filePath: String,
      partNumber: Int,
      bytes: Array[Byte],
      user: SessionUser = multipartOwnerSessionUser,
      contentLengthOverride: Option[Long] = None,
      missingContentLength: Boolean = false
  ): Response = {
    val hdrs =
      if (missingContentLength) mkHeadersMissingContentLength
      else mkHeaders(contentLengthOverride.getOrElse(bytes.length.toLong))

    datasetResource.uploadPart(
      ownerUser.getEmail,
      multipartDataset.getName,
      urlEnc(filePath),
      partNumber,
      new ByteArrayInputStream(bytes),
      hdrs,
      user
    )
  }

  private def uploadPartWithStream(
      filePath: String,
      partNumber: Int,
      stream: InputStream,
      contentLength: Long,
      user: SessionUser = multipartOwnerSessionUser
  ): Response =
    datasetResource.uploadPart(
      ownerUser.getEmail,
      multipartDataset.getName,
      urlEnc(filePath),
      partNumber,
      stream,
      mkHeaders(contentLength),
      user
    )

  private def fetchSession(filePath: String) =
    getDSLContext
      .selectFrom(DATASET_UPLOAD_SESSION)
      .where(
        DATASET_UPLOAD_SESSION.UID
          .eq(ownerUser.getUid)
          .and(DATASET_UPLOAD_SESSION.DID.eq(multipartDataset.getDid))
          .and(DATASET_UPLOAD_SESSION.FILE_PATH.eq(filePath))
      )
      .fetchOne()

  private def fetchPartRows(uploadId: String) =
    getDSLContext
      .selectFrom(DATASET_UPLOAD_SESSION_PART)
      .where(DATASET_UPLOAD_SESSION_PART.UPLOAD_ID.eq(uploadId))
      .fetch()
      .asScala
      .toList

  private def fetchUploadIdOrFail(filePath: String): String = {
    val sessionRecord = fetchSession(filePath)
    sessionRecord should not be null
    sessionRecord.getUploadId
  }

  private def assertPlaceholdersCreated(uploadId: String, expectedParts: Int): Unit = {
    val rows = fetchPartRows(uploadId).sortBy(_.getPartNumber)
    rows.size shouldEqual expectedParts
    rows.head.getPartNumber shouldEqual 1
    rows.last.getPartNumber shouldEqual expectedParts
    rows.foreach { r =>
      r.getEtag should not be null
      r.getEtag shouldEqual "" // placeholder convention
    }
  }

  private def assertStatus(ex: WebApplicationException, status: Int): Unit =
    ex.getResponse.getStatus shouldEqual status

  // ---------------------------------------------------------------------------
  // INIT TESTS
  // ---------------------------------------------------------------------------
  "multipart-upload?type=init" should "create an upload session row + precreate part placeholders (happy path)" in {
    val filePath = uniqueFilePath("init-happy")
    val resp = initUpload(filePath, numParts = 3)

    resp.getStatus shouldEqual 200

    val sessionRecord = fetchSession(filePath)
    sessionRecord should not be null
    sessionRecord.getNumPartsRequested shouldEqual 3
    sessionRecord.getUploadId should not be null
    sessionRecord.getPhysicalAddress should not be null

    assertPlaceholdersCreated(sessionRecord.getUploadId, expectedParts = 3)
  }

  it should "reject missing numParts" in {
    val filePath = uniqueFilePath("init-missing-numparts")
    val ex = intercept[BadRequestException] {
      datasetResource.multipartUpload(
        "init",
        ownerUser.getEmail,
        multipartDataset.getName,
        urlEnc(filePath),
        Optional.empty(),
        multipartOwnerSessionUser
      )
    }
    assertStatus(ex, 400)
  }

  it should "reject invalid numParts (0, negative, too large)" in {
    val filePath = uniqueFilePath("init-bad-numparts")
    assertStatus(intercept[BadRequestException] { initUpload(filePath, 0) }, 400)
    assertStatus(intercept[BadRequestException] { initUpload(filePath, -1) }, 400)
    assertStatus(intercept[BadRequestException] { initUpload(filePath, 1000000000) }, 400)
  }

  it should "reject invalid filePath (empty, absolute, '.', '..', control chars)" in {
    assertStatus(intercept[BadRequestException] { initUpload("./nope.bin", 2) }, 400)
    assertStatus(intercept[BadRequestException] { initUpload("/absolute.bin", 2) }, 400)
    assertStatus(intercept[BadRequestException] { initUpload("a/./b.bin", 2) }, 400)

    assertStatus(intercept[BadRequestException] { initUpload("../escape.bin", 2) }, 400)
    assertStatus(intercept[BadRequestException] { initUpload("a/../escape.bin", 2) }, 400)

    assertStatus(
      intercept[BadRequestException] {
        initUpload(s"a/${0.toChar}b.bin", 2)
      },
      400
    )
  }

  it should "reject invalid type parameter" in {
    val filePath = uniqueFilePath("init-bad-type")
    val ex = intercept[BadRequestException] {
      datasetResource.multipartUpload(
        "not-a-real-type",
        ownerUser.getEmail,
        multipartDataset.getName,
        urlEnc(filePath),
        Optional.empty(),
        multipartOwnerSessionUser
      )
    }
    assertStatus(ex, 400)
  }

  it should "reject init when caller lacks WRITE access" in {
    val filePath = uniqueFilePath("init-forbidden")
    val ex = intercept[ForbiddenException] {
      initUpload(filePath, numParts = 2, user = multipartNoWriteSessionUser)
    }
    assertStatus(ex, 403)
  }

  it should "handle init race: exactly one succeeds, one gets 409 CONFLICT" in {
    val filePath = uniqueFilePath("init-race")
    val barrier = new CyclicBarrier(2)

    def callInit(): Either[Throwable, Response] =
      try {
        barrier.await()
        Right(initUpload(filePath, numParts = 2))
      } catch {
        case t: Throwable => Left(t)
      }

    val future1 = Future(callInit())
    val future2 = Future(callInit())
    val results = Await.result(Future.sequence(Seq(future1, future2)), 30.seconds)

    val oks = results.collect { case Right(r) if r.getStatus == 200 => r }
    val fails = results.collect { case Left(t) => t }

    oks.size shouldEqual 1
    fails.size shouldEqual 1

    fails.head match {
      case e: WebApplicationException => assertStatus(e, 409)
      case other =>
        fail(
          s"Expected WebApplicationException(CONFLICT), got: ${other.getClass} / ${other.getMessage}"
        )
    }

    val sessionRecord = fetchSession(filePath)
    sessionRecord should not be null
    assertPlaceholdersCreated(sessionRecord.getUploadId, expectedParts = 2)
  }

  it should "reject sequential double init with 409 CONFLICT" in {
    val filePath = uniqueFilePath("init-double")
    initUpload(filePath, numParts = 2).getStatus shouldEqual 200

    val ex = intercept[WebApplicationException] { initUpload(filePath, numParts = 2) }
    assertStatus(ex, 409)
  }

  // ---------------------------------------------------------------------------
  // PART UPLOAD TESTS
  // ---------------------------------------------------------------------------
  "multipart-upload/part" should "reject uploadPart if init was not called" in {
    val filePath = uniqueFilePath("part-no-init")
    val ex = intercept[NotFoundException] {
      uploadPart(filePath, partNumber = 1, bytes = Array[Byte](1, 2, 3))
    }
    assertStatus(ex, 404)
  }

  it should "reject missing/invalid Content-Length" in {
    val filePath = uniqueFilePath("part-bad-cl")
    initUpload(filePath, numParts = 2)

    assertStatus(
      intercept[BadRequestException] {
        uploadPart(
          filePath,
          partNumber = 1,
          bytes = Array[Byte](1, 2, 3),
          missingContentLength = true
        )
      },
      400
    )

    assertStatus(
      intercept[BadRequestException] {
        uploadPart(
          filePath,
          partNumber = 1,
          bytes = Array[Byte](1, 2, 3),
          contentLengthOverride = Some(0L)
        )
      },
      400
    )

    assertStatus(
      intercept[BadRequestException] {
        uploadPart(
          filePath,
          partNumber = 1,
          bytes = Array[Byte](1, 2, 3),
          contentLengthOverride = Some(-5L)
        )
      },
      400
    )
  }

  it should "reject null/empty filePath param early without depending on error text" in {
    val httpHeaders = mkHeaders(1L)

    val ex1 = intercept[BadRequestException] {
      datasetResource.uploadPart(
        ownerUser.getEmail,
        multipartDataset.getName,
        null,
        1,
        new ByteArrayInputStream(Array.emptyByteArray),
        httpHeaders,
        multipartOwnerSessionUser
      )
    }
    assertStatus(ex1, 400)

    val ex2 = intercept[BadRequestException] {
      datasetResource.uploadPart(
        ownerUser.getEmail,
        multipartDataset.getName,
        "",
        1,
        new ByteArrayInputStream(Array.emptyByteArray),
        httpHeaders,
        multipartOwnerSessionUser
      )
    }
    assertStatus(ex2, 400)
  }

  it should "reject invalid partNumber (< 1) and partNumber > requested" in {
    val filePath = uniqueFilePath("part-bad-pn")
    initUpload(filePath, numParts = 2)

    assertStatus(
      intercept[BadRequestException] {
        uploadPart(filePath, partNumber = 0, bytes = tinyBytes(1.toByte))
      },
      400
    )

    assertStatus(
      intercept[BadRequestException] {
        uploadPart(filePath, partNumber = 3, bytes = minPartBytes(2.toByte))
      },
      400
    )
  }

  it should "reject a non-final part smaller than the minimum size (without checking message)" in {
    val filePath = uniqueFilePath("part-too-small-nonfinal")
    initUpload(filePath, numParts = 2)

    val ex = intercept[BadRequestException] {
      uploadPart(filePath, partNumber = 1, bytes = tinyBytes(1.toByte))
    }
    assertStatus(ex, 400)

    val uploadId = fetchUploadIdOrFail(filePath)
    fetchPartRows(uploadId).find(_.getPartNumber == 1).get.getEtag shouldEqual ""
  }

  it should "upload a part successfully and persist its ETag into DATASET_UPLOAD_SESSION_PART" in {
    val filePath = uniqueFilePath("part-happy-db")
    initUpload(filePath, numParts = 2)

    val uploadId = fetchUploadIdOrFail(filePath)
    fetchPartRows(uploadId).find(_.getPartNumber == 1).get.getEtag shouldEqual ""

    val bytes = minPartBytes(7.toByte)
    uploadPart(filePath, partNumber = 1, bytes = bytes).getStatus shouldEqual 200

    val after = fetchPartRows(uploadId).find(_.getPartNumber == 1).get
    after.getEtag should not equal ""
  }

  it should "allow retrying the same part sequentially (no duplicates, etag ends non-empty)" in {
    val filePath = uniqueFilePath("part-retry")
    initUpload(filePath, numParts = 2)
    val uploadId = fetchUploadIdOrFail(filePath)

    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 1, minPartBytes(2.toByte)).getStatus shouldEqual 200

    val rows = fetchPartRows(uploadId).filter(_.getPartNumber == 1)
    rows.size shouldEqual 1
    rows.head.getEtag should not equal ""
  }

  it should "apply per-part locking: return 409 if that part row is locked by another uploader" in {
    val filePath = uniqueFilePath("part-lock")
    initUpload(filePath, numParts = 2)
    val uploadId = fetchUploadIdOrFail(filePath)

    val connectionProvider = getDSLContext.configuration().connectionProvider()
    val connection = connectionProvider.acquire()
    connection.setAutoCommit(false)

    try {
      val locking = DSL.using(connection, SQLDialect.POSTGRES)
      locking
        .selectFrom(DATASET_UPLOAD_SESSION_PART)
        .where(
          DATASET_UPLOAD_SESSION_PART.UPLOAD_ID
            .eq(uploadId)
            .and(DATASET_UPLOAD_SESSION_PART.PART_NUMBER.eq(1))
        )
        .forUpdate()
        .fetchOne()

      val ex = intercept[WebApplicationException] {
        uploadPart(filePath, 1, minPartBytes(1.toByte))
      }
      assertStatus(ex, 409)
    } finally {
      connection.rollback()
      connectionProvider.release(connection)
    }

    uploadPart(filePath, 1, minPartBytes(3.toByte)).getStatus shouldEqual 200
  }

  it should "not block other parts: locking part 1 does not prevent uploading part 2" in {
    val filePath = uniqueFilePath("part-lock-other-part")
    initUpload(filePath, numParts = 2)
    val uploadId = fetchUploadIdOrFail(filePath)

    val connectionProvider = getDSLContext.configuration().connectionProvider()
    val connection = connectionProvider.acquire()
    connection.setAutoCommit(false)

    try {
      val locking = DSL.using(connection, SQLDialect.POSTGRES)
      locking
        .selectFrom(DATASET_UPLOAD_SESSION_PART)
        .where(
          DATASET_UPLOAD_SESSION_PART.UPLOAD_ID
            .eq(uploadId)
            .and(DATASET_UPLOAD_SESSION_PART.PART_NUMBER.eq(1))
        )
        .forUpdate()
        .fetchOne()

      uploadPart(filePath, 2, tinyBytes(9.toByte)).getStatus shouldEqual 200
    } finally {
      connection.rollback()
      connectionProvider.release(connection)
    }
  }

  it should "reject uploadPart when caller lacks WRITE access" in {
    val filePath = uniqueFilePath("part-forbidden")
    initUpload(filePath, numParts = 2)

    val ex = intercept[ForbiddenException] {
      uploadPart(filePath, 1, minPartBytes(1.toByte), user = multipartNoWriteSessionUser)
    }
    assertStatus(ex, 403)
  }

  // ---------------------------------------------------------------------------
  // FINISH TESTS
  // ---------------------------------------------------------------------------
  "multipart-upload?type=finish" should "reject finish if init was not called" in {
    val filePath = uniqueFilePath("finish-no-init")
    val ex = intercept[NotFoundException] { finishUpload(filePath) }
    assertStatus(ex, 404)
  }

  it should "reject finish when no parts were uploaded (all placeholders empty) without checking messages" in {
    val filePath = uniqueFilePath("finish-no-parts")
    initUpload(filePath, numParts = 2)

    val ex = intercept[WebApplicationException] { finishUpload(filePath) }
    assertStatus(ex, 409)

    fetchSession(filePath) should not be null
  }

  it should "reject finish when some parts are missing (etag empty treated as missing)" in {
    val filePath = uniqueFilePath("finish-missing")
    initUpload(filePath, numParts = 3)

    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200

    val ex = intercept[WebApplicationException] { finishUpload(filePath) }
    assertStatus(ex, 409)

    val uploadId = fetchUploadIdOrFail(filePath)
    fetchPartRows(uploadId).find(_.getPartNumber == 2).get.getEtag shouldEqual ""
    fetchPartRows(uploadId).find(_.getPartNumber == 3).get.getEtag shouldEqual ""
  }

  it should "reject finish when extra part rows exist in DB (bypass endpoint) without checking messages" in {
    val filePath = uniqueFilePath("finish-extra-db")
    initUpload(filePath, numParts = 2)

    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 2, tinyBytes(2.toByte)).getStatus shouldEqual 200

    val sessionRecord = fetchSession(filePath)
    val uploadId = sessionRecord.getUploadId

    getDSLContext
      .insertInto(DATASET_UPLOAD_SESSION_PART)
      .set(DATASET_UPLOAD_SESSION_PART.UPLOAD_ID, uploadId)
      .set(DATASET_UPLOAD_SESSION_PART.PART_NUMBER, Integer.valueOf(3))
      .set(DATASET_UPLOAD_SESSION_PART.ETAG, "bogus-etag")
      .execute()

    val ex = intercept[WebApplicationException] { finishUpload(filePath) }
    assertStatus(ex, 500)

    fetchSession(filePath) should not be null
    fetchPartRows(uploadId).nonEmpty shouldEqual true
  }

  it should "finish successfully when all parts have non-empty etags; delete session + part rows" in {
    val filePath = uniqueFilePath("finish-happy")
    initUpload(filePath, numParts = 3)

    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 2, minPartBytes(2.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 3, tinyBytes(3.toByte)).getStatus shouldEqual 200

    val uploadId = fetchUploadIdOrFail(filePath)

    val resp = finishUpload(filePath)
    resp.getStatus shouldEqual 200

    fetchSession(filePath) shouldBe null
    fetchPartRows(uploadId) shouldBe empty
  }

  it should "be idempotent-ish: second finish should return NotFound after successful finish" in {
    val filePath = uniqueFilePath("finish-twice")
    initUpload(filePath, numParts = 1)
    uploadPart(filePath, 1, tinyBytes(1.toByte)).getStatus shouldEqual 200

    finishUpload(filePath).getStatus shouldEqual 200

    val ex = intercept[NotFoundException] { finishUpload(filePath) }
    assertStatus(ex, 404)
  }

  it should "reject finish when caller lacks WRITE access" in {
    val filePath = uniqueFilePath("finish-forbidden")
    initUpload(filePath, numParts = 1)
    uploadPart(filePath, 1, tinyBytes(1.toByte)).getStatus shouldEqual 200

    val ex = intercept[ForbiddenException] {
      finishUpload(filePath, user = multipartNoWriteSessionUser)
    }
    assertStatus(ex, 403)
  }

  it should "return 409 CONFLICT if the session row is locked by another finalizer/aborter" in {
    val filePath = uniqueFilePath("finish-lock-race")
    initUpload(filePath, numParts = 1)
    uploadPart(filePath, 1, tinyBytes(1.toByte)).getStatus shouldEqual 200

    val connectionProvider = getDSLContext.configuration().connectionProvider()
    val connection = connectionProvider.acquire()
    connection.setAutoCommit(false)

    try {
      val locking = DSL.using(connection, SQLDialect.POSTGRES)
      locking
        .selectFrom(DATASET_UPLOAD_SESSION)
        .where(
          DATASET_UPLOAD_SESSION.UID
            .eq(ownerUser.getUid)
            .and(DATASET_UPLOAD_SESSION.DID.eq(multipartDataset.getDid))
            .and(DATASET_UPLOAD_SESSION.FILE_PATH.eq(filePath))
        )
        .forUpdate()
        .fetchOne()

      val ex = intercept[WebApplicationException] { finishUpload(filePath) }
      assertStatus(ex, 409)
    } finally {
      connection.rollback()
      connectionProvider.release(connection)
    }
  }

  // ---------------------------------------------------------------------------
  // ABORT TESTS
  // ---------------------------------------------------------------------------
  "multipart-upload?type=abort" should "reject abort if init was not called" in {
    val filePath = uniqueFilePath("abort-no-init")
    val ex = intercept[NotFoundException] { abortUpload(filePath) }
    assertStatus(ex, 404)
  }

  it should "abort successfully; delete session + part rows" in {
    val filePath = uniqueFilePath("abort-happy")
    initUpload(filePath, numParts = 2)
    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200

    val uploadId = fetchUploadIdOrFail(filePath)

    abortUpload(filePath).getStatus shouldEqual 200

    fetchSession(filePath) shouldBe null
    fetchPartRows(uploadId) shouldBe empty
  }

  it should "reject abort when caller lacks WRITE access" in {
    val filePath = uniqueFilePath("abort-forbidden")
    initUpload(filePath, numParts = 1)

    val ex = intercept[ForbiddenException] {
      abortUpload(filePath, user = multipartNoWriteSessionUser)
    }
    assertStatus(ex, 403)
  }

  it should "return 409 CONFLICT if the session row is locked by another finalizer/aborter" in {
    val filePath = uniqueFilePath("abort-lock-race")
    initUpload(filePath, numParts = 1)

    val connectionProvider = getDSLContext.configuration().connectionProvider()
    val connection = connectionProvider.acquire()
    connection.setAutoCommit(false)

    try {
      val locking = DSL.using(connection, SQLDialect.POSTGRES)
      locking
        .selectFrom(DATASET_UPLOAD_SESSION)
        .where(
          DATASET_UPLOAD_SESSION.UID
            .eq(ownerUser.getUid)
            .and(DATASET_UPLOAD_SESSION.DID.eq(multipartDataset.getDid))
            .and(DATASET_UPLOAD_SESSION.FILE_PATH.eq(filePath))
        )
        .forUpdate()
        .fetchOne()

      val ex = intercept[WebApplicationException] { abortUpload(filePath) }
      assertStatus(ex, 409)
    } finally {
      connection.rollback()
      connectionProvider.release(connection)
    }
  }

  it should "be consistent: abort after finish should return NotFound" in {
    val filePath = uniqueFilePath("abort-after-finish")
    initUpload(filePath, numParts = 1)
    uploadPart(filePath, 1, tinyBytes(1.toByte)).getStatus shouldEqual 200

    finishUpload(filePath).getStatus shouldEqual 200

    val ex = intercept[NotFoundException] { abortUpload(filePath) }
    assertStatus(ex, 404)
  }

  // ---------------------------------------------------------------------------
  // FAILURE / RESILIENCE (still unit tests; simulated failures)
  // ---------------------------------------------------------------------------
  "multipart upload implementation" should "release locks and keep DB consistent if the incoming stream fails mid-upload (simulated network drop)" in {
    val filePath = uniqueFilePath("netfail-upload-stream")
    initUpload(filePath, numParts = 2).getStatus shouldEqual 200
    val uploadId = fetchUploadIdOrFail(filePath)

    val payload = minPartBytes(5.toByte)

    val flaky = new InputStream {
      private var pos = 0
      override def read(): Int = {
        if (pos >= 1024) throw new IOException("simulated network drop")
        val b = payload(pos) & 0xff
        pos += 1
        b
      }
    }

    intercept[Throwable] {
      uploadPartWithStream(
        filePath,
        partNumber = 1,
        stream = flaky,
        contentLength = payload.length.toLong
      )
    }

    fetchPartRows(uploadId).find(_.getPartNumber == 1).get.getEtag shouldEqual ""

    uploadPart(filePath, 1, payload).getStatus shouldEqual 200
    fetchPartRows(uploadId).find(_.getPartNumber == 1).get.getEtag should not equal ""
  }

  it should "not delete session/parts if finalize fails downstream (simulate by corrupting an ETag)" in {
    val filePath = uniqueFilePath("netfail-finish")
    initUpload(filePath, numParts = 2).getStatus shouldEqual 200

    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 2, tinyBytes(2.toByte)).getStatus shouldEqual 200

    val uploadId = fetchUploadIdOrFail(filePath)

    getDSLContext
      .update(DATASET_UPLOAD_SESSION_PART)
      .set(DATASET_UPLOAD_SESSION_PART.ETAG, "definitely-not-a-real-etag")
      .where(
        DATASET_UPLOAD_SESSION_PART.UPLOAD_ID
          .eq(uploadId)
          .and(DATASET_UPLOAD_SESSION_PART.PART_NUMBER.eq(1))
      )
      .execute()

    intercept[Throwable] { finishUpload(filePath) }

    fetchSession(filePath) should not be null
    fetchPartRows(uploadId).nonEmpty shouldEqual true
  }

  it should "allow abort + re-init after part 1 succeeded but part 2 drops mid-flight; then complete successfully" in {
    val filePath = uniqueFilePath("reinit-after-part2-drop")

    initUpload(filePath, numParts = 2).getStatus shouldEqual 200
    val uploadId1 = fetchUploadIdOrFail(filePath)

    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200

    val bytesPart2 = Array.fill[Byte](1024 * 1024)(2.toByte)
    intercept[Throwable] {
      uploadPartWithStream(
        filePath,
        partNumber = 2,
        stream = flakyStream(bytesPart2, failAfterBytes = 4096),
        contentLength = bytesPart2.length.toLong
      )
    }

    abortUpload(filePath).getStatus shouldEqual 200
    fetchSession(filePath) shouldBe null
    fetchPartRows(uploadId1) shouldBe empty

    initUpload(filePath, numParts = 2).getStatus shouldEqual 200
    uploadPart(filePath, 1, minPartBytes(3.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 2, tinyBytes(4.toByte, n = 123)).getStatus shouldEqual 200
    finishUpload(filePath).getStatus shouldEqual 200
    fetchSession(filePath) shouldBe null
  }

  it should "allow re-upload after failures: (1) part1 drop, (2) part2 drop, (3) finalize failure; each followed by abort + re-init + success" in {
    def abortAndAssertClean(filePath: String, uploadId: String): Unit = {
      abortUpload(filePath).getStatus shouldEqual 200
      fetchSession(filePath) shouldBe null
      fetchPartRows(uploadId) shouldBe empty
    }

    def reinitAndFinishHappy(filePath: String): Unit = {
      initUpload(filePath, numParts = 2).getStatus shouldEqual 200
      uploadPart(filePath, 1, minPartBytes(7.toByte)).getStatus shouldEqual 200
      uploadPart(filePath, 2, tinyBytes(8.toByte, n = 321)).getStatus shouldEqual 200
      finishUpload(filePath).getStatus shouldEqual 200
      fetchSession(filePath) shouldBe null
    }

    withClue("scenario (1): part1 mid-flight drop") {
      val filePath = uniqueFilePath("reupload-part1-drop")
      initUpload(filePath, numParts = 2).getStatus shouldEqual 200
      val uploadId = fetchUploadIdOrFail(filePath)

      val p1 = minPartBytes(5.toByte)
      intercept[Throwable] {
        uploadPartWithStream(
          filePath,
          partNumber = 1,
          stream = flakyStream(p1, failAfterBytes = 4096),
          contentLength = p1.length.toLong
        )
      }

      fetchPartRows(uploadId).find(_.getPartNumber == 1).get.getEtag shouldEqual ""

      abortAndAssertClean(filePath, uploadId)
      reinitAndFinishHappy(filePath)
    }

    withClue("scenario (2): part2 mid-flight drop") {
      val filePath = uniqueFilePath("reupload-part2-drop")
      initUpload(filePath, numParts = 2).getStatus shouldEqual 200
      val uploadId = fetchUploadIdOrFail(filePath)

      uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
      val bytesPart2 = Array.fill[Byte](1024 * 1024)(2.toByte)
      intercept[Throwable] {
        uploadPartWithStream(
          filePath,
          partNumber = 2,
          stream = flakyStream(bytesPart2, failAfterBytes = 4096),
          contentLength = bytesPart2.length.toLong
        )
      }

      abortAndAssertClean(filePath, uploadId)
      reinitAndFinishHappy(filePath)
    }

    withClue("scenario (3): finalize failure then re-upload") {
      val filePath = uniqueFilePath("reupload-finalize-fail")
      initUpload(filePath, numParts = 2).getStatus shouldEqual 200

      uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
      uploadPart(filePath, 2, tinyBytes(2.toByte)).getStatus shouldEqual 200

      val uploadId = fetchUploadIdOrFail(filePath)
      getDSLContext
        .update(DATASET_UPLOAD_SESSION_PART)
        .set(DATASET_UPLOAD_SESSION_PART.ETAG, "definitely-not-a-real-etag")
        .where(
          DATASET_UPLOAD_SESSION_PART.UPLOAD_ID
            .eq(uploadId)
            .and(DATASET_UPLOAD_SESSION_PART.PART_NUMBER.eq(1))
        )
        .execute()

      intercept[Throwable] { finishUpload(filePath) }
      fetchSession(filePath) should not be null
      fetchPartRows(uploadId).nonEmpty shouldEqual true

      abortAndAssertClean(filePath, uploadId)
      reinitAndFinishHappy(filePath)
    }
  }

  // ---------------------------------------------------------------------------
  // CORRUPTION CHECKS
  // ---------------------------------------------------------------------------
  it should "upload without corruption (sha256 matches final object)" in {
    val filePath = uniqueFilePath("sha256-positive")
    initUpload(filePath, numParts = 3).getStatus shouldEqual 200

    val part1 = minPartBytes(1.toByte)
    val part2 = minPartBytes(2.toByte)
    val part3 = Array.fill[Byte](123)(3.toByte)

    uploadPart(filePath, 1, part1).getStatus shouldEqual 200
    uploadPart(filePath, 2, part2).getStatus shouldEqual 200
    uploadPart(filePath, 3, part3).getStatus shouldEqual 200

    finishUpload(filePath).getStatus shouldEqual 200

    val expected = sha256OfChunks(Seq(part1, part2, part3))

    val repoName = multipartDataset.getRepositoryName
    val ref = "main"
    val downloaded = LakeFSStorageClient.getFileFromRepo(repoName, ref, filePath)

    val got = sha256OfFile(Paths.get(downloaded.toURI))
    got.toSeq shouldEqual expected.toSeq
  }

  it should "detect corruption (sha256 mismatch when a part is altered)" in {
    val filePath = uniqueFilePath("sha256-negative")
    initUpload(filePath, numParts = 3).getStatus shouldEqual 200

    val part1 = minPartBytes(1.toByte)
    val part2 = minPartBytes(2.toByte)
    val part3 = Array.fill[Byte](123)(3.toByte)

    val intendedHash = sha256OfChunks(Seq(part1, part2, part3))

    val part2corrupt = part2.clone()
    part2corrupt(0) = (part2corrupt(0) ^ 0x01).toByte

    uploadPart(filePath, 1, part1).getStatus shouldEqual 200
    uploadPart(filePath, 2, part2corrupt).getStatus shouldEqual 200
    uploadPart(filePath, 3, part3).getStatus shouldEqual 200

    finishUpload(filePath).getStatus shouldEqual 200

    val repoName = multipartDataset.getRepositoryName
    val ref = "main"
    val downloaded = LakeFSStorageClient.getFileFromRepo(repoName, ref, filePath)

    val gotHash = sha256OfFile(Paths.get(downloaded.toURI))
    gotHash.toSeq should not equal intendedHash.toSeq

    val corruptHash = sha256OfChunks(Seq(part1, part2corrupt, part3))
    gotHash.toSeq shouldEqual corruptHash.toSeq
  }

  // ---------------------------------------------------------------------------
  // STRESS / SOAK TESTS (tagged)
  // ---------------------------------------------------------------------------
  it should "survive 2 concurrent multipart uploads (fan-out)" taggedAs (StressMultipart, Slow) in {
    val parallelUploads = 2
    val maxParts = 2

    def oneUpload(i: Int): Future[Unit] =
      Future {
        val filePath = uniqueFilePath(s"stress-$i")
        val numParts = 2 + Random.nextInt(maxParts - 1)

        initUpload(filePath, numParts).getStatus shouldEqual 200

        val sharedMin = minPartBytes((i % 127).toByte)
        val partFuts = (1 to numParts).map { partN =>
          Future {
            val bytes =
              if (partN < numParts) sharedMin
              else tinyBytes((partN % 127).toByte, n = 1024)
            uploadPart(filePath, partN, bytes).getStatus shouldEqual 200
          }
        }

        Await.result(Future.sequence(partFuts), 60.seconds)

        finishUpload(filePath).getStatus shouldEqual 200
        fetchSession(filePath) shouldBe null
      }

    val all = Future.sequence((1 to parallelUploads).map(oneUpload))
    Await.result(all, 180.seconds)
  }

  it should "throttle concurrent uploads of the SAME part via per-part locks" taggedAs (StressMultipart, Slow) in {
    val filePath = uniqueFilePath("stress-same-part")
    initUpload(filePath, numParts = 2).getStatus shouldEqual 200

    val contenders = 2
    val barrier = new CyclicBarrier(contenders)

    def tryUploadStatus(): Future[Int] =
      Future {
        barrier.await()
        try {
          uploadPart(filePath, 1, minPartBytes(7.toByte)).getStatus
        } catch {
          case e: WebApplicationException => e.getResponse.getStatus
        }
      }

    val statuses =
      Await.result(Future.sequence((1 to contenders).map(_ => tryUploadStatus())), 60.seconds)

    statuses.foreach { s => s should (be(200) or be(409)) }
    statuses.count(_ == 200) should be >= 1

    val uploadId = fetchUploadIdOrFail(filePath)
    val part1 = fetchPartRows(uploadId).find(_.getPartNumber == 1).get
    part1.getEtag.trim should not be ""
  }
}
