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
import jakarta.ws.rs.core._
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSession.DATASET_UPLOAD_SESSION
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSessionPart.DATASET_UPLOAD_SESSION_PART
import org.apache.texera.dao.jooq.generated.tables.daos.{DatasetDao, DatasetVersionDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, DatasetVersion, User}
import org.apache.texera.service.MockLakeFS
import org.apache.texera.service.util.S3StorageClient
import org.jooq.SQLDialect
import org.jooq.impl.DSL
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.tagobjects.Slow
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Tag}
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

  // ---------- Response entity helpers ----------
  private def entityAsScalaMap(resp: Response): Map[String, Any] = {
    resp.getEntity match {
      case m: scala.collection.Map[_, _] =>
        m.asInstanceOf[scala.collection.Map[String, Any]].toMap
      case m: java.util.Map[_, _] =>
        m.asScala.toMap.asInstanceOf[Map[String, Any]]
      case null => Map.empty
      case other =>
        fail(s"Unexpected response entity type: ${other.getClass}")
    }
  }

  private def mapListOfInts(x: Any): List[Int] =
    x match {
      case l: java.util.List[_]       => l.asScala.map(_.toString.toInt).toList
      case l: scala.collection.Seq[_] => l.map(_.toString.toInt).toList
      case other                      => fail(s"Expected list, got: ${other.getClass}")
    }

  private def mapListOfStrings(x: Any): List[String] =
    x match {
      case l: java.util.List[_]       => l.asScala.map(_.toString).toList
      case l: scala.collection.Seq[_] => l.map(_.toString).toList
      case other                      => fail(s"Expected list, got: ${other.getClass}")
    }

  private def listUploads(
      user: SessionUser = multipartOwnerSessionUser
  ): List[String] = {
    val resp = datasetResource.multipartUpload(
      "list",
      ownerUser.getEmail,
      multipartDataset.getName,
      urlEnc("ignored"),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      user
    )
    resp.getStatus shouldEqual 200
    val m = entityAsScalaMap(resp)
    mapListOfStrings(m("filePaths"))
  }

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

  // Test fixtures for cover image tests. Creates file in LakeFS and DatasetVersion record.
  private val testCoverImagePath = "v1/test-cover.jpg"
  private val testImageBytes: Array[Byte] = Array.fill[Byte](1024)(0xff.toByte)

  private lazy val testDatasetVersion: DatasetVersion = {
    try {
      LakeFSStorageClient.initRepo(baseDataset.getRepositoryName)
    } catch {
      case e: ApiException if e.getCode == 409 =>
    }

    LakeFSStorageClient.writeFileToRepo(
      baseDataset.getRepositoryName,
      "test-cover.jpg",
      new ByteArrayInputStream(testImageBytes)
    )

    val version = new DatasetVersion()
    version.setDid(baseDataset.getDid)
    version.setCreatorUid(ownerUser.getUid)
    version.setName("v1")
    version.setVersionHash("main")

    new DatasetVersionDao(getDSLContext.configuration()).insert(version)
    version
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
    // Ensure max upload size setting does not leak between tests
    clearMaxUploadMiB()
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
  private def mkHeadersRawContentLength(raw: String): HttpHeaders =
    new HttpHeaders {
      override def getRequestHeader(name: String): java.util.List[String] =
        if (HttpHeaders.CONTENT_LENGTH.equalsIgnoreCase(name)) Collections.singletonList(raw)
        else Collections.emptyList()

      override def getHeaderString(name: String): String =
        if (HttpHeaders.CONTENT_LENGTH.equalsIgnoreCase(name)) raw else null
      override def getRequestHeaders: MultivaluedMap[String, String] = {
        val map = new MultivaluedHashMap[String, String]()
        map.putSingle(HttpHeaders.CONTENT_LENGTH, raw)
        map
      }
      override def getAcceptableMediaTypes: java.util.List[MediaType] = Collections.emptyList()
      override def getAcceptableLanguages: java.util.List[Locale] = Collections.emptyList()
      override def getMediaType: MediaType = null
      override def getLanguage: Locale = null
      override def getCookies: java.util.Map[String, Cookie] = Collections.emptyMap()
      // Not used by the resource (it reads getHeaderString), but keep it safe.
      override def getLength: Int = -1

      override def getDate: Date = ???
    }
  private def uniqueFilePath(prefix: String): String =
    s"$prefix/${System.nanoTime()}-${Random.alphanumeric.take(8).mkString}.bin"

  // ---------- site_settings helpers (max upload size) ----------
  private val MaxUploadKey = "single_file_upload_max_size_mib"

  private def upsertSiteSetting(key: String, value: String): Unit = {
    val table = DSL.table(DSL.name("texera_db", "site_settings"))
    val keyField = DSL.field(DSL.name("key"), classOf[String])
    val valField = DSL.field(DSL.name("value"), classOf[String])

    // Keep it simple + compatible across jOOQ versions: delete then insert.
    val ctx = getDSLContext
    ctx.deleteFrom(table).where(keyField.eq(key)).execute()
    ctx.insertInto(table).columns(keyField, valField).values(key, value).execute()
  }

  private def deleteSiteSetting(key: String): Boolean = {
    val table = DSL.table(DSL.name("texera_db", "site_settings"))
    val keyField = DSL.field(DSL.name("key"), classOf[String])
    getDSLContext.deleteFrom(table).where(keyField.eq(key)).execute() > 0
  }

  private def setMaxUploadMiB(mib: Long): Unit = upsertSiteSetting(MaxUploadKey, mib.toString)
  private def clearMaxUploadMiB(): Unit = deleteSiteSetting(MaxUploadKey)

  /**
    * Convenience helper that adapts legacy "numParts" tests to the new init API:
    * init now takes (fileSizeBytes, partSizeBytes) and computes numParts internally.
    *
    * - Non-final parts are exactly partSizeBytes.
    * - Final part is exactly lastPartBytes.
    */
  private def initUpload(
      filePath: String,
      numParts: Int,
      lastPartBytes: Int = 1,
      partSizeBytes: Int = MinNonFinalPartBytes,
      user: SessionUser = multipartOwnerSessionUser,
      restart: Optional[java.lang.Boolean] = Optional.empty()
  ): Response = {
    require(numParts >= 1, "numParts must be >= 1")
    require(lastPartBytes > 0, "lastPartBytes must be > 0")
    require(partSizeBytes > 0, "partSizeBytes must be > 0")
    if (numParts > 1)
      require(
        lastPartBytes <= partSizeBytes,
        "lastPartBytes must be <= partSizeBytes for multipart"
      )

    val fileSizeBytes: Long =
      if (numParts == 1) lastPartBytes.toLong
      else partSizeBytes.toLong * (numParts.toLong - 1L) + lastPartBytes.toLong

    // For numParts == 1, allow partSizeBytes >= fileSizeBytes (still computes 1 part).
    val maxPartSizeBytes: Long =
      if (numParts == 1) Math.max(partSizeBytes.toLong, fileSizeBytes) else partSizeBytes.toLong

    datasetResource.multipartUpload(
      "init",
      ownerUser.getEmail,
      multipartDataset.getName,
      urlEnc(filePath),
      Optional.of(java.lang.Long.valueOf(fileSizeBytes)),
      Optional.of(java.lang.Long.valueOf(maxPartSizeBytes)),
      restart,
      user
    )
  }
  private def initRaw(
      filePath: String,
      fileSizeBytes: Long,
      partSizeBytes: Long,
      user: SessionUser = multipartOwnerSessionUser
  ): Response = {
    datasetResource.multipartUpload(
      "init",
      ownerUser.getEmail,
      multipartDataset.getName,
      urlEnc(filePath),
      Optional.of(java.lang.Long.valueOf(fileSizeBytes)),
      Optional.of(java.lang.Long.valueOf(partSizeBytes)),
      Optional.empty(),
      user
    )
  }

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
      Optional.empty(),
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
      Optional.empty(),
      Optional.empty(),
      user
    )

  private def uploadPart(
      filePath: String,
      partNumber: Int,
      bytes: Array[Byte],
      user: SessionUser = multipartOwnerSessionUser,
      contentLengthOverride: Option[Long] = None,
      missingContentLength: Boolean = false,
      rawContentLengthOverride: Option[String] = None
  ): Response = {
    val contentLength = contentLengthOverride.getOrElse(bytes.length.toLong)
    val headers =
      if (missingContentLength) mkHeadersMissingContentLength
      else
        rawContentLengthOverride.map(mkHeadersRawContentLength).getOrElse(mkHeaders(contentLength))

    datasetResource.uploadPart(
      ownerUser.getEmail,
      multipartDataset.getName,
      urlEnc(filePath),
      partNumber,
      new ByteArrayInputStream(bytes),
      headers,
      user
    )
  }

  private def uploadPartWithStream(
      filePath: String,
      partNumber: Int,
      stream: InputStream,
      contentLength: Long,
      user: SessionUser = multipartOwnerSessionUser,
      rawContentLengthOverride: Option[String] = None
  ): Response = {
    val headers =
      rawContentLengthOverride.map(mkHeadersRawContentLength).getOrElse(mkHeaders(contentLength))
    datasetResource.uploadPart(
      ownerUser.getEmail,
      multipartDataset.getName,
      urlEnc(filePath),
      partNumber,
      stream,
      headers,
      user
    )
  }

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
  // LIST TESTS (type=list)
  // ---------------------------------------------------------------------------
  "multipart-upload?type=list" should "return empty when no active sessions exist for the dataset" in {
    // Make deterministic: remove any leftover sessions from other tests.
    getDSLContext
      .deleteFrom(DATASET_UPLOAD_SESSION)
      .where(DATASET_UPLOAD_SESSION.DID.eq(multipartDataset.getDid))
      .execute()

    listUploads() shouldBe empty
  }

  it should "reject list when caller lacks WRITE access" in {
    val ex = intercept[ForbiddenException] {
      listUploads(user = multipartNoWriteSessionUser)
    }
    ex.getResponse.getStatus shouldEqual 403
  }

  it should "return only non-expired sessions, sorted by filePath (and exclude expired ones)" in {
    // Clean slate
    getDSLContext
      .deleteFrom(DATASET_UPLOAD_SESSION)
      .where(DATASET_UPLOAD_SESSION.DID.eq(multipartDataset.getDid))
      .execute()

    val fpA = uniqueFilePath("list-a")
    val fpB = uniqueFilePath("list-b")

    initUpload(fpB, numParts = 2).getStatus shouldEqual 200
    initUpload(fpA, numParts = 2).getStatus shouldEqual 200

    // Expire fpB by pushing created_at back > 6 hours.
    val uploadIdB = fetchUploadIdOrFail(fpB)
    val tableName = DATASET_UPLOAD_SESSION.getName // typically "dataset_upload_session"
    getDSLContext
      .update(DATASET_UPLOAD_SESSION)
      .set(
        DATASET_UPLOAD_SESSION.CREATED_AT,
        DSL.field("current_timestamp - interval '7 hours'").cast(classOf[java.time.OffsetDateTime])
      )
      .where(DATASET_UPLOAD_SESSION.UPLOAD_ID.eq(uploadIdB))
      .execute()

    val listed = listUploads()
    listed shouldEqual listed.sorted
    listed should contain(fpA)
    listed should not contain fpB
  }

  it should "not list sessions after abort (cleanup works end-to-end)" in {
    // Clean slate
    getDSLContext
      .deleteFrom(DATASET_UPLOAD_SESSION)
      .where(DATASET_UPLOAD_SESSION.DID.eq(multipartDataset.getDid))
      .execute()

    val fp = uniqueFilePath("list-after-abort")
    initUpload(fp, numParts = 2).getStatus shouldEqual 200

    listUploads() should contain(fp)

    abortUpload(fp).getStatus shouldEqual 200

    listUploads() should not contain fp
  }

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
  it should "restart session when restart=true is explicitly requested (even if config is unchanged) and reset progress" in {
    val filePath = uniqueFilePath("init-restart-true")

    // Initial init
    initUpload(filePath, numParts = 2, lastPartBytes = 123).getStatus shouldEqual 200
    val oldUploadId = fetchUploadIdOrFail(filePath)

    // Make progress in old session
    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    fetchPartRows(oldUploadId).find(_.getPartNumber == 1).get.getEtag.trim should not be ""

    // Re-init with same config but restart=true => must restart
    val r2 = initUpload(
      filePath,
      numParts = 2,
      lastPartBytes = 123,
      restart = Optional.of(java.lang.Boolean.TRUE)
    )
    r2.getStatus shouldEqual 200

    val newUploadId = fetchUploadIdOrFail(filePath)
    newUploadId should not equal oldUploadId

    // Old part rows gone, new placeholders empty
    fetchPartRows(oldUploadId) shouldBe empty
    assertPlaceholdersCreated(newUploadId, expectedParts = 2)

    // Response should look like a fresh session
    val m = entityAsScalaMap(r2)
    mapListOfInts(m("missingParts")) shouldEqual List(1, 2)
    m("completedPartsCount").toString.toInt shouldEqual 0
  }

  it should "not restart session when restart=false (same config) and preserve uploadId + progress" in {
    val filePath = uniqueFilePath("init-restart-false")

    initUpload(filePath, numParts = 3, lastPartBytes = 123).getStatus shouldEqual 200
    val uploadId1 = fetchUploadIdOrFail(filePath)

    uploadPart(filePath, 1, minPartBytes(7.toByte)).getStatus shouldEqual 200

    val r2 = initUpload(
      filePath,
      numParts = 3,
      lastPartBytes = 123,
      restart = Optional.of(java.lang.Boolean.FALSE)
    )
    r2.getStatus shouldEqual 200

    val uploadId2 = fetchUploadIdOrFail(filePath)
    uploadId2 shouldEqual uploadId1

    val m = entityAsScalaMap(r2)
    mapListOfInts(m("missingParts")) shouldEqual List(2, 3)
    m("completedPartsCount").toString.toInt shouldEqual 1
  }

  it should "restart even when all parts were already uploaded (restart=true makes missingParts full again)" in {
    val filePath = uniqueFilePath("init-restart-after-all-parts")

    initUpload(filePath, numParts = 2, lastPartBytes = 123).getStatus shouldEqual 200
    val oldUploadId = fetchUploadIdOrFail(filePath)

    // Upload everything (but don't finish)
    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 2, tinyBytes(2.toByte, n = 123)).getStatus shouldEqual 200

    // Confirm "all done" without restart
    val rNoRestart = initUpload(filePath, numParts = 2, lastPartBytes = 123)
    rNoRestart.getStatus shouldEqual 200
    val mNoRestart = entityAsScalaMap(rNoRestart)
    mapListOfInts(mNoRestart("missingParts")) shouldEqual Nil
    mNoRestart("completedPartsCount").toString.toInt shouldEqual 2

    // Now force restart => must reset
    val rRestart = initUpload(
      filePath,
      numParts = 2,
      lastPartBytes = 123,
      restart = Optional.of(java.lang.Boolean.TRUE)
    )
    rRestart.getStatus shouldEqual 200

    val newUploadId = fetchUploadIdOrFail(filePath)
    newUploadId should not equal oldUploadId
    fetchPartRows(oldUploadId) shouldBe empty
    assertPlaceholdersCreated(newUploadId, expectedParts = 2)

    val m = entityAsScalaMap(rRestart)
    mapListOfInts(m("missingParts")) shouldEqual List(1, 2)
    m("completedPartsCount").toString.toInt shouldEqual 0
  }

  "multipart-upload?type=init" should "restart session when init config changes (fileSize/partSize/numParts) and recreate placeholders" in {
    val filePath = uniqueFilePath("init-conflict-restart")

    // First init => 2 parts
    initUpload(filePath, numParts = 2, lastPartBytes = 123).getStatus shouldEqual 200
    val oldUploadId = fetchUploadIdOrFail(filePath)

    // Upload part 1 so old session isn't empty
    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    fetchPartRows(oldUploadId).find(_.getPartNumber == 1).get.getEtag.trim should not be ""

    // Second init with DIFFERENT config => 3 parts now
    val resp2 = initUpload(filePath, numParts = 3, lastPartBytes = 50)
    resp2.getStatus shouldEqual 200

    val newUploadId = fetchUploadIdOrFail(filePath)
    newUploadId should not equal oldUploadId

    // Old part rows should have been deleted via ON DELETE CASCADE
    fetchPartRows(oldUploadId) shouldBe empty

    // New placeholders should exist and be empty
    assertPlaceholdersCreated(newUploadId, expectedParts = 3)

    val m2 = entityAsScalaMap(resp2)
    mapListOfInts(m2("missingParts")) shouldEqual List(1, 2, 3)
    m2("completedPartsCount").toString.toInt shouldEqual 0
  }

  it should "restart session when physicalAddress has expired (created_at too old), even if config is unchanged" in {
    val filePath = uniqueFilePath("init-expired-restart")

    // First init (2 parts)
    val r1 = initUpload(filePath, numParts = 2, lastPartBytes = 123)
    r1.getStatus shouldEqual 200

    val oldUploadId = fetchUploadIdOrFail(filePath)
    oldUploadId should not be null

    // Optional: create some progress so we know it truly resets
    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    fetchPartRows(oldUploadId).find(_.getPartNumber == 1).get.getEtag.trim should not be ""

    // Age the session so it is definitely expired (> PHYSICAL_ADDRESS_EXPIRATION_TIME_HRS = 6)
    val expireHrs = S3StorageClient.PHYSICAL_ADDRESS_EXPIRATION_TIME_HRS

    getDSLContext
      .update(DATASET_UPLOAD_SESSION)
      .set(
        DATASET_UPLOAD_SESSION.CREATED_AT,
        DSL
          .field(s"current_timestamp - interval '${expireHrs + 1} hours'")
          .cast(classOf[java.time.OffsetDateTime])
      )
      .where(DATASET_UPLOAD_SESSION.UPLOAD_ID.eq(oldUploadId))
      .execute()

    // Same init config again -> should restart because it's expired
    val r2 = initUpload(filePath, numParts = 2, lastPartBytes = 123)
    r2.getStatus shouldEqual 200

    val newUploadId = fetchUploadIdOrFail(filePath)
    newUploadId should not equal oldUploadId

    // Old part rows should have been deleted (ON DELETE CASCADE)
    fetchPartRows(oldUploadId) shouldBe empty

    // New placeholders should exist, empty
    assertPlaceholdersCreated(newUploadId, expectedParts = 2)

    // Response should reflect a fresh session
    val m2 = entityAsScalaMap(r2)
    mapListOfInts(m2("missingParts")) shouldEqual List(1, 2)
    m2("completedPartsCount").toString.toInt shouldEqual 0
  }

  it should "be resumable: repeated init with same config keeps uploadId and returns missingParts + completedPartsCount" in {
    val filePath = uniqueFilePath("init-resume-same-config")

    val resp1 = initUpload(filePath, numParts = 3, lastPartBytes = 123)
    resp1.getStatus shouldEqual 200
    val uploadId1 = fetchUploadIdOrFail(filePath)

    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200

    val resp2 = initUpload(filePath, numParts = 3, lastPartBytes = 123)
    resp2.getStatus shouldEqual 200
    val uploadId2 = fetchUploadIdOrFail(filePath)

    uploadId2 shouldEqual uploadId1

    val m2 = entityAsScalaMap(resp2)
    val missing = mapListOfInts(m2("missingParts"))
    missing shouldEqual List(2, 3)
    m2("completedPartsCount").toString.toInt shouldEqual 1
  }
  it should "return missingParts=[] when all parts are already uploaded (completedPartsCount == numParts)" in {
    val filePath = uniqueFilePath("init-all-done")
    initUpload(filePath, numParts = 2, lastPartBytes = 123).getStatus shouldEqual 200

    uploadPart(filePath, 1, minPartBytes(7.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 2, tinyBytes(8.toByte, n = 123)).getStatus shouldEqual 200

    val resp2 = initUpload(filePath, numParts = 2, lastPartBytes = 123)
    resp2.getStatus shouldEqual 200

    val m2 = entityAsScalaMap(resp2)
    mapListOfInts(m2("missingParts")) shouldEqual Nil
    m2("completedPartsCount").toString.toInt shouldEqual 2
  }
  it should "return 409 CONFLICT if the upload session row is locked by another transaction" in {
    val filePath = uniqueFilePath("init-session-row-locked")
    initUpload(filePath, numParts = 2).getStatus shouldEqual 200

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

      val ex = intercept[WebApplicationException] {
        initUpload(filePath, numParts = 2)
      }
      ex.getResponse.getStatus shouldEqual 409
    } finally {
      connection.rollback()
      connectionProvider.release(connection)
    }

    // lock released => init works again
    initUpload(filePath, numParts = 2).getStatus shouldEqual 200
  }
  it should "treat normalized-equivalent paths as the same session (no duplicate sessions)" in {
    val base = s"norm-${System.nanoTime()}.bin"
    val raw = s"a/../$base" // normalizes to base

    // init using traversal-ish but normalizable path
    initUpload(raw, numParts = 1, lastPartBytes = 16, partSizeBytes = 16).getStatus shouldEqual 200
    val uploadId1 = fetchUploadIdOrFail(base) // stored path should be normalized

    // init using normalized path should hit the same session (resume)
    val resp2 = initUpload(base, numParts = 1, lastPartBytes = 16, partSizeBytes = 16)
    resp2.getStatus shouldEqual 200
    val uploadId2 = fetchUploadIdOrFail(base)

    uploadId2 shouldEqual uploadId1

    val m2 = entityAsScalaMap(resp2)
    mapListOfInts(m2("missingParts")) shouldEqual List(1)
    m2("completedPartsCount").toString.toInt shouldEqual 0
  }
  it should "restart session when fileSizeBytes differs (single-part; computedNumParts unchanged)" in {
    val filePath = uniqueFilePath("init-conflict-filesize")

    val declared = 16
    val r1 = initRaw(filePath, fileSizeBytes = declared, partSizeBytes = 32L) // numParts=1
    r1.getStatus shouldEqual 200
    val oldUploadId = fetchUploadIdOrFail(filePath)

    // Add progress in old session
    uploadPart(filePath, 1, Array.fill[Byte](declared)(1.toByte)).getStatus shouldEqual 200

    fetchPartRows(oldUploadId).find(_.getPartNumber == 1).get.getEtag.trim should not be ""

    val r2 = initRaw(filePath, fileSizeBytes = 17L, partSizeBytes = 32L) // numParts=1 still
    r2.getStatus shouldEqual 200
    val newUploadId = fetchUploadIdOrFail(filePath)

    newUploadId should not equal oldUploadId
    fetchPartRows(oldUploadId) shouldBe empty // old placeholders removed

    val session = fetchSession(filePath)
    session.getFileSizeBytes shouldEqual 17L
    session.getPartSizeBytes shouldEqual 32L
    session.getNumPartsRequested shouldEqual 1

    val m = entityAsScalaMap(r2)
    mapListOfInts(m("missingParts")) shouldEqual List(1)
    m("completedPartsCount").toString.toInt shouldEqual 0 // progress reset
  }

  it should "restart session when partSizeBytes differs (single-part; computedNumParts unchanged)" in {
    val filePath = uniqueFilePath("init-conflict-partsize")

    initRaw(filePath, fileSizeBytes = 16L, partSizeBytes = 32L).getStatus shouldEqual 200
    val oldUploadId = fetchUploadIdOrFail(filePath)

    // Second init, same fileSize, different partSize, still 1 part
    val r2 = initRaw(filePath, fileSizeBytes = 16L, partSizeBytes = 64L)
    r2.getStatus shouldEqual 200
    val newUploadId = fetchUploadIdOrFail(filePath)

    newUploadId should not equal oldUploadId
    fetchPartRows(oldUploadId) shouldBe empty

    val session = fetchSession(filePath)
    session.getFileSizeBytes shouldEqual 16L
    session.getPartSizeBytes shouldEqual 64L
    session.getNumPartsRequested shouldEqual 1

    val m = entityAsScalaMap(r2)
    mapListOfInts(m("missingParts")) shouldEqual List(1)
    m("completedPartsCount").toString.toInt shouldEqual 0
  }
  it should "restart session when computed numParts differs (multipart -> single-part)" in {
    val filePath = uniqueFilePath("init-conflict-numparts")

    val partSize = MinNonFinalPartBytes.toLong // 5 MiB
    val fileSize = partSize * 2L + 123L // => computedNumParts = 3

    val r1 = initRaw(filePath, fileSizeBytes = fileSize, partSizeBytes = partSize)
    r1.getStatus shouldEqual 200
    val oldUploadId = fetchUploadIdOrFail(filePath)
    fetchSession(filePath).getNumPartsRequested shouldEqual 3

    // Create progress
    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200

    // Re-init with a partSize >= fileSize => computedNumParts becomes 1
    val r2 = initRaw(filePath, fileSizeBytes = fileSize, partSizeBytes = fileSize)
    r2.getStatus shouldEqual 200
    val newUploadId = fetchUploadIdOrFail(filePath)

    newUploadId should not equal oldUploadId
    fetchPartRows(oldUploadId) shouldBe empty

    val session = fetchSession(filePath)
    session.getNumPartsRequested shouldEqual 1
    session.getFileSizeBytes shouldEqual fileSize
    session.getPartSizeBytes shouldEqual fileSize

    val m = entityAsScalaMap(r2)
    mapListOfInts(m("missingParts")) shouldEqual List(1)
    m("completedPartsCount").toString.toInt shouldEqual 0
  }

  it should "reject missing fileSizeBytes / partSizeBytes" in {
    val filePath1 = uniqueFilePath("init-missing-filesize")
    val ex1 = intercept[BadRequestException] {
      datasetResource.multipartUpload(
        "init",
        ownerUser.getEmail,
        multipartDataset.getName,
        urlEnc(filePath1),
        Optional.empty(),
        Optional.of(java.lang.Long.valueOf(MinNonFinalPartBytes.toLong)),
        Optional.empty(),
        multipartOwnerSessionUser
      )
    }
    assertStatus(ex1, 400)

    val filePath2 = uniqueFilePath("init-missing-partsize")
    val ex2 = intercept[BadRequestException] {
      datasetResource.multipartUpload(
        "init",
        ownerUser.getEmail,
        multipartDataset.getName,
        urlEnc(filePath2),
        Optional.of(java.lang.Long.valueOf(1L)),
        Optional.empty(),
        Optional.empty(),
        multipartOwnerSessionUser
      )
    }
    assertStatus(ex2, 400)
  }

  it should "reject invalid fileSizeBytes / partSizeBytes (<= 0)" in {
    val filePath = uniqueFilePath("init-bad-sizes")

    assertStatus(
      intercept[BadRequestException] {
        datasetResource.multipartUpload(
          "init",
          ownerUser.getEmail,
          multipartDataset.getName,
          urlEnc(filePath),
          Optional.of(java.lang.Long.valueOf(0L)),
          Optional.of(java.lang.Long.valueOf(1L)),
          Optional.empty(),
          multipartOwnerSessionUser
        )
      },
      400
    )

    assertStatus(
      intercept[BadRequestException] {
        datasetResource.multipartUpload(
          "init",
          ownerUser.getEmail,
          multipartDataset.getName,
          urlEnc(filePath),
          Optional.of(java.lang.Long.valueOf(1L)),
          Optional.of(java.lang.Long.valueOf(0L)),
          Optional.empty(),
          multipartOwnerSessionUser
        )
      },
      400
    )
  }

  it should "enforce max upload size at init (>, == boundary)" in {
    // Use a tiny limit so the test doesn't allocate big buffers.
    setMaxUploadMiB(1) // 1 MiB

    val oneMiB: Long = 1024L * 1024L

    val filePathOver = uniqueFilePath("init-max-over")
    assertStatus(
      intercept[BadRequestException] {
        datasetResource.multipartUpload(
          "init",
          ownerUser.getEmail,
          multipartDataset.getName,
          urlEnc(filePathOver),
          Optional.of(java.lang.Long.valueOf(oneMiB + 1L)),
          Optional.of(java.lang.Long.valueOf(oneMiB + 1L)), // single-part
          Optional.empty(),
          multipartOwnerSessionUser
        )
      },
      400
    )

    val filePathEq = uniqueFilePath("init-max-eq")
    val resp =
      datasetResource.multipartUpload(
        "init",
        ownerUser.getEmail,
        multipartDataset.getName,
        urlEnc(filePathEq),
        Optional.of(java.lang.Long.valueOf(oneMiB)),
        Optional.of(java.lang.Long.valueOf(oneMiB)), // single-part
        Optional.empty(),
        multipartOwnerSessionUser
      )

    resp.getStatus shouldEqual 200
    fetchSession(filePathEq) should not be null
  }

  it should "enforce max upload size for multipart (2-part boundary)" in {
    setMaxUploadMiB(6) // 6 MiB

    val max6MiB: Long = 6L * 1024L * 1024L
    val partSize: Long = MinNonFinalPartBytes.toLong // 5 MiB

    val filePathEq = uniqueFilePath("init-max-multipart-eq")
    val respEq =
      datasetResource.multipartUpload(
        "init",
        ownerUser.getEmail,
        multipartDataset.getName,
        urlEnc(filePathEq),
        Optional.of(java.lang.Long.valueOf(max6MiB)),
        Optional.of(java.lang.Long.valueOf(partSize)),
        Optional.empty(),
        multipartOwnerSessionUser
      )

    respEq.getStatus shouldEqual 200
    fetchSession(filePathEq).getNumPartsRequested shouldEqual 2

    val filePathOver = uniqueFilePath("init-max-multipart-over")
    assertStatus(
      intercept[BadRequestException] {
        datasetResource.multipartUpload(
          "init",
          ownerUser.getEmail,
          multipartDataset.getName,
          urlEnc(filePathOver),
          Optional.of(java.lang.Long.valueOf(max6MiB + 1L)),
          Optional.of(java.lang.Long.valueOf(partSize)),
          Optional.empty(),
          multipartOwnerSessionUser
        )
      },
      400
    )
  }

  it should "reject init when fileSizeBytes/partSizeBytes would overflow numParts computation (malicious huge inputs)" in {
    // Make max big enough to get past the max-size gate without overflowing maxBytes itself.
    val maxMiB: Long = Long.MaxValue / (1024L * 1024L)
    setMaxUploadMiB(maxMiB)
    val totalMaxBytes: Long = maxMiB * 1024L * 1024L
    val filePath = uniqueFilePath("init-overflow-numParts")

    val ex = intercept[WebApplicationException] {
      datasetResource.multipartUpload(
        "init",
        ownerUser.getEmail,
        multipartDataset.getName,
        urlEnc(filePath),
        Optional.of(java.lang.Long.valueOf(totalMaxBytes)),
        Optional.of(java.lang.Long.valueOf(MinNonFinalPartBytes.toLong)),
        Optional.empty(),
        multipartOwnerSessionUser
      )
    }
    assertStatus(ex, 500)
  }

  it should "reject invalid filePath (empty, absolute, '..', control chars)" in {
    // failures (must throw)
    assertStatus(intercept[BadRequestException] { initUpload("/absolute.bin", 2) }, 400)
    assertStatus(intercept[BadRequestException] { initUpload("../escape.bin", 2) }, 400)
    // control chars rejected
    intercept[IllegalArgumentException] {
      initUpload(s"a/${0.toChar}b.bin", 2)
    }

    // now succeed (no intercept, because no throw)
    assert(initUpload("./nope.bin", 2).getStatus == 200)
    assert(initUpload("a/./b.bin", 2).getStatus == 200)
    assert(initUpload("a/../escape.bin", 2).getStatus == 200)
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
        Optional.empty(),
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

  it should "handle init race: concurrent init calls converge to a single session (both return 200)" in {
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

    // No unexpected failures
    val fails = results.collect { case Left(t) => t }
    withClue(s"init race failures: ${fails.map(_.getMessage).mkString(", ")}") {
      fails shouldBe empty
    }

    // Both should be OK
    val oks = results.collect { case Right(r) => r }
    oks.size shouldEqual 2
    oks.foreach(_.getStatus shouldEqual 200)

    // Exactly one session row exists for this file path
    val sessionRecord = fetchSession(filePath)
    sessionRecord should not be null

    // Placeholders created for expected parts
    assertPlaceholdersCreated(sessionRecord.getUploadId, expectedParts = 2)

    //Both responses should report missingParts [1,2] and completedPartsCount 0
    oks.foreach { r =>
      val m = entityAsScalaMap(r)
      mapListOfInts(m("missingParts")) shouldEqual List(1, 2)
      m("completedPartsCount").toString.toInt shouldEqual 0
    }
  }

  it should "return 409 if init cannot acquire the session row lock (NOWAIT)" in {
    val filePath = uniqueFilePath("init-lock-409")
    initUpload(filePath, numParts = 2).getStatus shouldEqual 200

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

      val ex = intercept[WebApplicationException] {
        initUpload(filePath, numParts = 2)
      }
      ex.getResponse.getStatus shouldEqual 409
    } finally {
      connection.rollback()
      connectionProvider.release(connection)
    }
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
  it should "reject non-numeric Content-Length (header poisoning)" in {
    val filePath = uniqueFilePath("part-cl-nonnumeric")
    initUpload(filePath, numParts = 1)
    val ex = intercept[BadRequestException] {
      uploadPart(
        filePath,
        partNumber = 1,
        bytes = tinyBytes(1.toByte),
        rawContentLengthOverride = Some("not-a-number")
      )
    }
    assertStatus(ex, 400)
  }
  it should "reject Content-Length that overflows Long (header poisoning)" in {
    val filePath = uniqueFilePath("part-cl-overflow")
    initUpload(filePath, numParts = 1)
    val ex = intercept[BadRequestException] {
      uploadPart(
        filePath,
        partNumber = 1,
        bytes = tinyBytes(1.toByte),
        rawContentLengthOverride = Some("999999999999999999999999999999999999999")
      )
    }
    assertStatus(ex, 400)
  }
  it should "reject when Content-Length does not equal the expected part size (attempted size-bypass)" in {
    val filePath = uniqueFilePath("part-cl-mismatch-expected")
    initUpload(filePath, numParts = 2)
    val uploadId = fetchUploadIdOrFail(filePath)
    val bytes = minPartBytes(1.toByte) // exactly MinNonFinalPartBytes
    val ex = intercept[BadRequestException] {
      uploadPart(
        filePath,
        partNumber = 1,
        bytes = bytes,
        contentLengthOverride = Some(bytes.length.toLong - 1L) // lie by 1 byte
      )
    }
    assertStatus(ex, 400)
    // Ensure we didn't accidentally persist an ETag for a rejected upload.
    fetchPartRows(uploadId).find(_.getPartNumber == 1).get.getEtag shouldEqual ""
  }

  it should "not store more bytes than declared Content-Length (send 2x bytes, claim x)" in {
    val filePath = uniqueFilePath("part-body-gt-cl")
    val declared: Int = 1024
    initUpload(filePath, numParts = 1, lastPartBytes = declared, partSizeBytes = declared)

    val first = Array.fill[Byte](declared)(1.toByte)
    val extra = Array.fill[Byte](declared)(2.toByte)
    val sent = first ++ extra // 2x bytes sent

    uploadPart(
      filePath,
      partNumber = 1,
      bytes = sent,
      contentLengthOverride = Some(declared.toLong) // claim only x
    ).getStatus shouldEqual 200

    finishUpload(filePath).getStatus shouldEqual 200
    // If anything "accepted" the extra bytes, the committed object would exceed declared size.
    val repoName = multipartDataset.getRepositoryName
    val downloaded = LakeFSStorageClient.getFileFromRepo(repoName, "main", filePath)
    Files.size(Paths.get(downloaded.toURI)) shouldEqual declared.toLong

    val expected = sha256OfChunks(Seq(first))
    val got = sha256OfFile(Paths.get(downloaded.toURI))
    got.toSeq shouldEqual expected
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

  "multipart-upload/part" should "treat retries as idempotent once ETag is set (no overwrite on second call)" in {
    val filePath = uniqueFilePath("part-idempotent")
    initUpload(
      filePath,
      numParts = 1,
      lastPartBytes = 16,
      partSizeBytes = 16
    ).getStatus shouldEqual 200

    val uploadId = fetchUploadIdOrFail(filePath)

    val n = 16
    val bytes1: Array[Byte] = Array.tabulate[Byte](n)(i => (i + 1).toByte)
    val bytes2: Array[Byte] = Array.tabulate[Byte](n)(i => (i + 1).toByte)

    uploadPart(filePath, 1, bytes1).getStatus shouldEqual 200
    val etag1 = fetchPartRows(uploadId).find(_.getPartNumber == 1).get.getEtag

    uploadPart(filePath, 1, bytes2).getStatus shouldEqual 200
    val etag2 = fetchPartRows(uploadId).find(_.getPartNumber == 1).get.getEtag

    etag2 shouldEqual etag1

    finishUpload(filePath).getStatus shouldEqual 200

    val repoName = multipartDataset.getRepositoryName
    val downloaded = LakeFSStorageClient.getFileFromRepo(repoName, "main", filePath)
    val gotBytes = Files.readAllBytes(Paths.get(downloaded.toURI))

    gotBytes.toSeq shouldEqual bytes1.toSeq
  }

  // ---------------------------------------------------------------------------
  // FINISH TESTS
  // ---------------------------------------------------------------------------
  "multipart-upload?type=finish" should "reject finish if init was not called" in {
    val filePath = uniqueFilePath("finish-no-init")
    val ex = intercept[NotFoundException] { finishUpload(filePath) }
    assertStatus(ex, 404)
  }

  it should "not commit an oversized upload if the max upload size is tightened before finish (server-side rollback)" in {
    val filePath = uniqueFilePath("finish-max-tightened")
    val twoMiB: Long = 2L * 1024L * 1024L

    // Allow init + part upload under a higher limit.
    setMaxUploadMiB(3) // 3 MiB
    datasetResource
      .multipartUpload(
        "init",
        ownerUser.getEmail,
        multipartDataset.getName,
        urlEnc(filePath),
        Optional.of(java.lang.Long.valueOf(twoMiB)),
        Optional.of(java.lang.Long.valueOf(twoMiB)),
        Optional.empty(),
        multipartOwnerSessionUser
      )
      .getStatus shouldEqual 200

    uploadPart(filePath, 1, Array.fill[Byte](twoMiB.toInt)(7.toByte)).getStatus shouldEqual 200

    // Tighten the limit just before finish.
    setMaxUploadMiB(1) // 1 MiB

    val ex = intercept[WebApplicationException] {
      finishUpload(filePath) // this now THROWS 413 (doesn't return Response)
    }
    ex.getResponse.getStatus shouldEqual 413

    // Oversized objects must not remain accessible after finish (rollback happened).
    val repoName = multipartDataset.getRepositoryName
    val notFound = intercept[ApiException] {
      LakeFSStorageClient.getFileFromRepo(repoName, "main", filePath)
    }
    notFound.getCode shouldEqual 404

    // Session still available.
    fetchSession(filePath) should not be null
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

    initUpload(filePath, numParts = 2, lastPartBytes = 1024 * 1024).getStatus shouldEqual 200
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

    initUpload(filePath, numParts = 2, lastPartBytes = 123).getStatus shouldEqual 200
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
      initUpload(filePath, numParts = 2, lastPartBytes = 321).getStatus shouldEqual 200
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
      initUpload(filePath, numParts = 2, lastPartBytes = 1024 * 1024).getStatus shouldEqual 200
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
    initUpload(filePath, numParts = 3, lastPartBytes = 123).getStatus shouldEqual 200

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
    initUpload(filePath, numParts = 3, lastPartBytes = 123).getStatus shouldEqual 200

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

        initUpload(filePath, numParts, lastPartBytes = 1024).getStatus shouldEqual 200

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

  // ===========================================================================
  // Cover Image Tests
  // ===========================================================================

  "updateDatasetCoverImage" should "reject path traversal attempts" in {
    val maliciousPaths = Seq(
      "../../../etc/passwd",
      "v1/../../secret.txt",
      "../escape.jpg"
    )

    maliciousPaths.foreach { path =>
      val request = DatasetResource.CoverImageRequest(path)

      assertThrows[BadRequestException] {
        datasetResource.updateDatasetCoverImage(
          baseDataset.getDid,
          request,
          sessionUser
        )
      }
    }
  }

  it should "reject absolute paths" in {
    val absolutePaths = Seq(
      "/etc/passwd",
      "/var/log/system.log"
    )

    absolutePaths.foreach { path =>
      val request = DatasetResource.CoverImageRequest(path)

      assertThrows[BadRequestException] {
        datasetResource.updateDatasetCoverImage(
          baseDataset.getDid,
          request,
          sessionUser
        )
      }
    }
  }

  it should "reject invalid file types" in {
    val invalidPaths = Seq(
      "v1/script.js",
      "v1/document.pdf",
      "v1/data.csv"
    )

    invalidPaths.foreach { path =>
      val request = DatasetResource.CoverImageRequest(path)

      assertThrows[BadRequestException] {
        datasetResource.updateDatasetCoverImage(
          baseDataset.getDid,
          request,
          sessionUser
        )
      }
    }
  }

  it should "reject empty or null cover image path" in {
    assertThrows[BadRequestException] {
      datasetResource.updateDatasetCoverImage(
        baseDataset.getDid,
        DatasetResource.CoverImageRequest(""),
        sessionUser
      )
    }

    assertThrows[BadRequestException] {
      datasetResource.updateDatasetCoverImage(
        baseDataset.getDid,
        DatasetResource.CoverImageRequest(null),
        sessionUser
      )
    }
  }

  it should "reject when user lacks WRITE access" in {
    val request = DatasetResource.CoverImageRequest("v1/cover.jpg")

    assertThrows[ForbiddenException] {
      datasetResource.updateDatasetCoverImage(
        baseDataset.getDid,
        request,
        sessionUser2
      )
    }
  }

  it should "set cover image successfully" in {
    testDatasetVersion

    val request = DatasetResource.CoverImageRequest(testCoverImagePath)
    val response = datasetResource.updateDatasetCoverImage(
      baseDataset.getDid,
      request,
      sessionUser
    )

    response.getStatus shouldEqual 200

    val updated = datasetDao.fetchOneByDid(baseDataset.getDid)
    updated.getCoverImage shouldEqual testCoverImagePath
  }

  "getDatasetCover" should "reject private dataset cover for anonymous users" in {
    val dataset = datasetDao.fetchOneByDid(baseDataset.getDid)
    dataset.setIsPublic(false)
    dataset.setCoverImage("v1/cover.jpg")
    datasetDao.update(dataset)

    assertThrows[ForbiddenException] {
      datasetResource.getDatasetCover(baseDataset.getDid, Optional.empty())
    }
  }

  it should "reject private dataset cover for users without access" in {
    val dataset = datasetDao.fetchOneByDid(baseDataset.getDid)
    dataset.setOwnerUid(ownerUser.getUid)
    dataset.setIsPublic(false)
    dataset.setCoverImage("v1/cover.jpg")
    datasetDao.update(dataset)

    assertThrows[ForbiddenException] {
      datasetResource.getDatasetCover(baseDataset.getDid, Optional.of(sessionUser2))
    }
  }

  it should "return 404 when no cover image is set" in {
    val dataset = datasetDao.fetchOneByDid(baseDataset.getDid)
    dataset.setCoverImage(null)
    dataset.setIsPublic(true)
    datasetDao.update(dataset)

    assertThrows[NotFoundException] {
      datasetResource.getDatasetCover(baseDataset.getDid, Optional.of(sessionUser))
    }
  }

  it should "get cover image successfully with 307 redirect" in {
    testDatasetVersion

    val dataset = datasetDao.fetchOneByDid(baseDataset.getDid)
    dataset.setIsPublic(true)
    dataset.setCoverImage(testCoverImagePath)
    datasetDao.update(dataset)

    val response = datasetResource.getDatasetCover(
      baseDataset.getDid,
      Optional.empty()
    )

    response.getStatus shouldEqual 307
    response.getHeaderString("Location") should not be null
  }

  "LakeFS error handling" should "return 500 when ETag is invalid, with the message included in the error response body" in {
    val filePath = uniqueFilePath("error-body")

    initUpload(filePath, 2).getStatus shouldEqual 200
    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 2, tinyBytes(2.toByte)).getStatus shouldEqual 200

    val uploadId = fetchUploadIdOrFail(filePath)
    getDSLContext
      .update(DATASET_UPLOAD_SESSION_PART)
      .set(DATASET_UPLOAD_SESSION_PART.ETAG, "BAD")
      .where(DATASET_UPLOAD_SESSION_PART.UPLOAD_ID.eq(uploadId))
      .execute()

    val ex = intercept[WebApplicationException] {
      finishUpload(filePath)
    }

    ex.getResponse.getStatus shouldEqual 500
    Option(ex.getResponse.getEntity).map(_.toString).getOrElse("") should include(
      "LakeFS request failed due to an unexpected server error."
    )

    abortUpload(filePath)
  }

  it should "return 400 when physicalAddress is invalid" in {
    val filePath = uniqueFilePath("missing-physical-address")

    initUpload(filePath, 2).getStatus shouldEqual 200
    uploadPart(filePath, 1, minPartBytes(1.toByte)).getStatus shouldEqual 200
    uploadPart(filePath, 2, tinyBytes(2.toByte)).getStatus shouldEqual 200

    val uploadId = fetchUploadIdOrFail(filePath)

    getDSLContext
      .update(DATASET_UPLOAD_SESSION)
      .set(DATASET_UPLOAD_SESSION.PHYSICAL_ADDRESS, "BAD")
      .where(DATASET_UPLOAD_SESSION.UPLOAD_ID.eq(uploadId))
      .execute()

    val ex = intercept[WebApplicationException] { finishUpload(filePath) }
    ex.getResponse.getStatus shouldEqual 400
    Option(ex.getResponse.getEntity).map(_.toString).getOrElse("") should include(
      "LakeFS rejected the request"
    )

    intercept[WebApplicationException] {
      abortUpload(filePath)
    }.getResponse.getStatus shouldEqual 400

    // DB session is cleaned up
    fetchSession(filePath) shouldBe null
    fetchPartRows(uploadId) shouldBe empty
  }
}
