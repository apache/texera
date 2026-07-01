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

package org.apache.texera.service.util

import io.lakefs.clients.sdk.ApiException
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSession.DATASET_UPLOAD_SESSION
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSessionPart.DATASET_UPLOAD_SESSION_PART
import org.apache.texera.dao.jooq.generated.tables.daos.{DatasetDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, User}
import org.apache.texera.service.MockLakeFS
import org.apache.texera.service.resource.DatasetResource
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.io.ByteArrayInputStream
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.util.Optional

/**
  * Spec for [[StagedFileCleanupJob]] (issue #3681 — automated cleanup of uploaded but
  * uncommitted files).
  *
  * Contract under test:
  *   - `runCleanupOnce(now)` deletes DATASET_UPLOAD_SESSION rows whose created_at is older
  *     than `retentionHours` relative to the injected `now` (aborting their LakeFS multipart
  *     first; part rows go away via ON DELETE CASCADE),
  *   - resets LakeFS staged (uncommitted) objects whose mtime exceeds retention,
  *   - skips staged objects that belong to a non-expired upload session,
  *   - never touches committed objects,
  *   - counts per-item failures in `errors` without aborting the batch,
  *   - is idempotent.
  *
  * Tests never sleep to age things: sessions are aged either by passing a future `now`
  * (everything created "now" is then older than retention) or by writing an explicit
  * created_at via jOOQ. Staged-object mtimes cannot be faked, so object-expiry tests pass a
  * future `now`, and "fresh staged object" semantics under a future `now` are exercised via
  * the session-protection rule (created_at moved next to the future `now`).
  */
class StagedFileCleanupJobSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with MockLakeFS
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  // ---------------------------------------------------------------------------
  // Job configuration under test
  // ---------------------------------------------------------------------------
  private val RetentionHours = 24
  private val IntervalMinutes = 60

  private lazy val job = new StagedFileCleanupJob(RetentionHours, IntervalMinutes)

  /** A `now` far enough in the future that anything created at real wall-clock time is expired. */
  private def farFuture: OffsetDateTime =
    OffsetDateTime.now().plusHours(RetentionHours.toLong + 1L)

  // ---------------------------------------------------------------------------
  // Fixtures (minimal copies of the DatasetResourceSpec idioms)
  // ---------------------------------------------------------------------------
  private val ownerUser: User = {
    val user = new User
    user.setName("cleanup_test_user")
    user.setPassword("123")
    user.setEmail("cleanup_test_user@test.com")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  private val repoName: String = s"cleanup-ds-${System.nanoTime()}"

  private val cleanupDataset: Dataset = {
    val dataset = new Dataset
    dataset.setName("cleanup-ds")
    dataset.setRepositoryName(repoName)
    dataset.setIsPublic(true)
    dataset.setIsDownloadable(true)
    dataset.setDescription("dataset for staged-file cleanup tests")
    dataset
  }

  /** Object committed once up-front; must survive every cleanup run (safety pin). */
  private val PinnedCommittedPath = "pinned/committed-pin.bin"

  private lazy val sessionUser = new SessionUser(ownerUser)
  private lazy val datasetResource = new DatasetResource()

  private var lakeFsReady = false

  // ---------------------------------------------------------------------------
  // Lifecycle
  // ---------------------------------------------------------------------------
  override protected def beforeAll(): Unit = {
    super.beforeAll()

    initializeDBAndReplaceDSLContext()

    new UserDao(getDSLContext.configuration()).insert(ownerUser)
    cleanupDataset.setOwnerUid(ownerUser.getUid)
    new DatasetDao(getDSLContext.configuration()).insert(cleanupDataset)
  }

  // Containers (MockLakeFS) only become reachable after the suite starts running, so all
  // LakeFS setup happens lazily here rather than in beforeAll — same reason
  // DatasetResourceSpec initializes its repo in beforeEach.
  override protected def beforeEach(): Unit = {
    super.beforeEach()

    if (!lakeFsReady) {
      try LakeFSStorageClient.initRepo(repoName)
      catch {
        case e: ApiException if e.getCode == 409 => // already exists, fine
      }
      // Commit one object up-front: cleanup must NEVER touch committed objects.
      LakeFSStorageClient.writeFileToRepo(
        repoName,
        PinnedCommittedPath,
        new ByteArrayInputStream("pinned".getBytes(StandardCharsets.UTF_8))
      )
      LakeFSStorageClient.createCommit(repoName, "main", "pin committed object")
      lakeFsReady = true
    }

    // Clean slate so report counts are exact and independent of test order.
    // (Deliberately NOT done via the job under test, to keep fixtures independent of it.)
    getDSLContext.deleteFrom(DATASET_UPLOAD_SESSION).execute()
    LakeFSStorageClient
      .retrieveUncommittedObjects(repoName)
      .foreach(diff => LakeFSStorageClient.resetObjectUploadOrDeletion(repoName, diff.getPath))
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------
  private def urlEnc(raw: String): String =
    URLEncoder.encode(raw, StandardCharsets.UTF_8.name())

  private def uniquePath(prefix: String): String =
    s"$prefix/${System.nanoTime()}.bin"

  /** Creates a real upload session (valid uploadId + physicalAddress) and returns its uploadId. */
  private def initSession(filePath: String): String = {
    val resp = datasetResource.multipartUpload(
      "init",
      ownerUser.getEmail,
      cleanupDataset.getName,
      urlEnc(filePath),
      Optional.of(java.lang.Long.valueOf(16L)),
      Optional.of(java.lang.Long.valueOf(32L)), // single part
      Optional.empty(),
      sessionUser
    )
    resp.getStatus shouldEqual 200
    val record = fetchSession(filePath)
    record should not be null
    record.getUploadId
  }

  private def fetchSession(filePath: String) =
    getDSLContext
      .selectFrom(DATASET_UPLOAD_SESSION)
      .where(
        DATASET_UPLOAD_SESSION.UID
          .eq(ownerUser.getUid)
          .and(DATASET_UPLOAD_SESSION.DID.eq(cleanupDataset.getDid))
          .and(DATASET_UPLOAD_SESSION.FILE_PATH.eq(filePath))
      )
      .fetchOne()

  private def countPartRows(uploadId: String): Int =
    getDSLContext
      .selectCount()
      .from(DATASET_UPLOAD_SESSION_PART)
      .where(DATASET_UPLOAD_SESSION_PART.UPLOAD_ID.eq(uploadId))
      .fetchOne(0, classOf[Int])

  private def countDatasetUploadSessions(): Int =
    getDSLContext
      .selectCount()
      .from(DATASET_UPLOAD_SESSION)
      .where(DATASET_UPLOAD_SESSION.DID.eq(cleanupDataset.getDid))
      .fetchOne(0, classOf[Int])

  /** Pins a session's age precisely — the injectable-clock counterpart on the DB side. */
  private def setSessionCreatedAt(uploadId: String, createdAt: OffsetDateTime): Unit =
    getDSLContext
      .update(DATASET_UPLOAD_SESSION)
      .set(DATASET_UPLOAD_SESSION.CREATED_AT, createdAt)
      .where(DATASET_UPLOAD_SESSION.UPLOAD_ID.eq(uploadId))
      .execute()

  /** Uploads an object to the repo branch WITHOUT committing (a staged/uncommitted object). */
  private def stageObject(filePath: String, content: String = "staged-bytes"): Unit =
    LakeFSStorageClient.writeFileToRepo(
      repoName,
      filePath,
      new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
    )

  private def uncommittedPaths(): List[String] =
    LakeFSStorageClient.retrieveUncommittedObjects(repoName).map(_.getPath)

  private def committedPaths(): List[String] =
    LakeFSStorageClient.retrieveObjectsOfVersion(repoName, "main").map(_.getPath)

  /** Inserts a session row whose LakeFS multipart does not exist (forces a per-item failure). */
  private def insertBogusSession(filePath: String): Unit =
    getDSLContext
      .insertInto(DATASET_UPLOAD_SESSION)
      .set(DATASET_UPLOAD_SESSION.DID, cleanupDataset.getDid)
      .set(DATASET_UPLOAD_SESSION.UID, ownerUser.getUid)
      .set(DATASET_UPLOAD_SESSION.FILE_PATH, filePath)
      .set(DATASET_UPLOAD_SESSION.UPLOAD_ID, s"bogus-upload-${System.nanoTime()}")
      .set(
        DATASET_UPLOAD_SESSION.PHYSICAL_ADDRESS,
        "s3://nonexistent-bucket/nonexistent-key"
      )
      .set(DATASET_UPLOAD_SESSION.NUM_PARTS_REQUESTED, Int.box(1))
      .set(DATASET_UPLOAD_SESSION.FILE_SIZE_BYTES, java.lang.Long.valueOf(16L))
      .set(DATASET_UPLOAD_SESSION.PART_SIZE_BYTES, java.lang.Long.valueOf(32L))
      .execute()

  // ===========================================================================
  // 1. Expired session is cleaned
  // ===========================================================================
  "StagedFileCleanupJob.runCleanupOnce" should "delete an expired upload session and its part rows" in {
    val filePath = uniquePath("expired-session")
    val uploadId = initSession(filePath)
    countPartRows(uploadId) shouldEqual 1 // placeholder created at init

    val report = job.runCleanupOnce(farFuture)

    report.sessionsDeleted shouldEqual 1
    report.errors shouldEqual 0
    fetchSession(filePath) shouldBe null
    countPartRows(uploadId) shouldEqual 0 // gone via ON DELETE CASCADE
  }

  // ===========================================================================
  // 2. Fresh session survives
  // ===========================================================================
  it should "keep a fresh (non-expired) upload session" in {
    val filePath = uniquePath("fresh-session")
    val uploadId = initSession(filePath)

    val report = job.runCleanupOnce(OffsetDateTime.now())

    report.sessionsDeleted shouldEqual 0
    report.errors shouldEqual 0
    fetchSession(filePath) should not be null
    countPartRows(uploadId) shouldEqual 1
  }

  // ===========================================================================
  // 3. Expired staged object is reset (committed object untouched)
  // ===========================================================================
  it should "reset an expired staged object but never a committed one" in {
    val stagedPath = uniquePath("expired-staged")
    stageObject(stagedPath)
    uncommittedPaths() should contain(stagedPath)

    val report = job.runCleanupOnce(farFuture)

    report.objectsReset should be >= 1
    report.errors shouldEqual 0
    uncommittedPaths() should not contain stagedPath
    // safety pin: the committed object survives every cleanup
    committedPaths() should contain(PinnedCommittedPath)
  }

  // ===========================================================================
  // 4. Fresh staged object survives
  // ===========================================================================
  it should "keep a freshly staged object when run with the real current time" in {
    val stagedPath = uniquePath("fresh-staged")
    stageObject(stagedPath)

    val report = job.runCleanupOnce(OffsetDateTime.now())

    report.objectsReset shouldEqual 0
    report.errors shouldEqual 0
    uncommittedPaths() should contain(stagedPath)
  }

  // ===========================================================================
  // 5. Idempotence
  // ===========================================================================
  it should "be idempotent: a second run with the same now reports all zeros" in {
    val sessionPath = uniquePath("idempotent-session")
    val stagedPath = uniquePath("idempotent-staged")
    initSession(sessionPath)
    stageObject(stagedPath)

    val now = farFuture

    val first = job.runCleanupOnce(now)
    first.sessionsDeleted shouldEqual 1
    first.objectsReset should be >= 1
    first.errors shouldEqual 0

    val second = job.runCleanupOnce(now)
    second.sessionsDeleted shouldEqual 0
    second.objectsReset shouldEqual 0
    second.errors shouldEqual 0
  }

  it should "process only a bounded number of expired sessions per cleanup round" in {
    val boundedJob =
      new StagedFileCleanupJob(RetentionHours, IntervalMinutes, sessionCleanupBatchSize = 1)

    initSession(uniquePath("bounded-session-1"))
    initSession(uniquePath("bounded-session-2"))
    countDatasetUploadSessions() shouldEqual 2

    val first = boundedJob.runCleanupOnce(farFuture)
    first.sessionsDeleted shouldEqual 1
    first.errors shouldEqual 0
    countDatasetUploadSessions() shouldEqual 1

    val second = boundedJob.runCleanupOnce(farFuture)
    second.sessionsDeleted shouldEqual 1
    second.errors shouldEqual 0
    countDatasetUploadSessions() shouldEqual 0
  }

  // ===========================================================================
  // 6. Active upload is not touched while other items expire
  // ===========================================================================
  it should "not touch a non-expired session or its staged object while expiring other items" in {
    val now = farFuture

    // Protected: a session that is fresh RELATIVE TO the injected now, with its staged
    // file present on the branch. The skip rule must protect the object even though its
    // real mtime is "older" than retention relative to the future now.
    val protectedPath = uniquePath("active-upload")
    val protectedUploadId = initSession(protectedPath)
    stageObject(protectedPath)
    setSessionCreatedAt(protectedUploadId, now.minusMinutes(5))

    // Expirees: another session and an orphan staged object, both created at real now,
    // i.e. older than retention relative to the future now.
    val expiredSessionPath = uniquePath("expired-other-session")
    initSession(expiredSessionPath)
    val expiredStagedPath = uniquePath("expired-other-staged")
    stageObject(expiredStagedPath)

    val report = job.runCleanupOnce(now)

    report.sessionsDeleted shouldEqual 1
    report.objectsReset shouldEqual 1
    report.errors shouldEqual 0

    // survivors
    fetchSession(protectedPath) should not be null
    uncommittedPaths() should contain(protectedPath)
    // expirees
    fetchSession(expiredSessionPath) shouldBe null
    uncommittedPaths() should not contain expiredStagedPath
  }

  // ===========================================================================
  // 7. Report counting on a mixed batch
  // ===========================================================================
  it should "report exact counts for a mix of expired and fresh items" in {
    val now = farFuture

    // 2 expired sessions
    val expired1 = uniquePath("mix-expired-1")
    val expired2 = uniquePath("mix-expired-2")
    initSession(expired1)
    initSession(expired2)

    // 1 fresh session protecting 1 fresh staged object (fresh relative to the injected now)
    val freshPath = uniquePath("mix-fresh")
    val freshUploadId = initSession(freshPath)
    stageObject(freshPath)
    setSessionCreatedAt(freshUploadId, now.minusMinutes(5))

    // 1 expired staged object with no session
    val expiredStaged = uniquePath("mix-expired-staged")
    stageObject(expiredStaged)

    val report = job.runCleanupOnce(now)

    report.sessionsDeleted shouldEqual 2
    report.objectsReset shouldEqual 1
    report.errors shouldEqual 0

    fetchSession(expired1) shouldBe null
    fetchSession(expired2) shouldBe null
    fetchSession(freshPath) should not be null
    uncommittedPaths() should contain(freshPath)
    uncommittedPaths() should not contain expiredStaged
  }

  // ===========================================================================
  // 8. Retention boundary (precision via injectable clock + explicit created_at)
  // ===========================================================================
  it should "clean a session just past retention but keep one just inside it" in {
    val now = OffsetDateTime.now()
    val cutoff = now.minusHours(RetentionHours.toLong)

    val survivorPath = uniquePath("boundary-survivor")
    val survivorUploadId = initSession(survivorPath)
    setSessionCreatedAt(survivorUploadId, cutoff.plusMinutes(1)) // retention - epsilon

    val expiredPath = uniquePath("boundary-expired")
    val expiredUploadId = initSession(expiredPath)
    setSessionCreatedAt(expiredUploadId, cutoff.minusMinutes(1)) // retention + epsilon

    val report = job.runCleanupOnce(now)

    report.sessionsDeleted shouldEqual 1
    report.errors shouldEqual 0
    fetchSession(survivorPath) should not be null
    fetchSession(expiredPath) shouldBe null
  }

  // ===========================================================================
  // 9. Committed objects are never touched (dedicated, with a fresh commit)
  // ===========================================================================
  it should "leave committed objects intact while resetting expired staged objects" in {
    // Commit a new object in this test, alongside an expired staged object.
    val committedPath = uniquePath("committed-safe")
    stageObject(committedPath, content = "committed-bytes")
    LakeFSStorageClient.createCommit(repoName, "main", "commit object that cleanup must keep")

    val expiredStaged = uniquePath("doomed-staged")
    stageObject(expiredStaged)

    val report = job.runCleanupOnce(farFuture)

    report.errors shouldEqual 0
    uncommittedPaths() should not contain expiredStaged
    committedPaths() should contain(committedPath)
    committedPaths() should contain(PinnedCommittedPath)
  }

  // ===========================================================================
  // 10. Already-aborted multipart (LakeFS 404) is treated as cleaned, not an error
  // ===========================================================================
  it should "delete a session whose multipart was already aborted in LakeFS, with no error" in {
    val filePath = uniquePath("already-aborted")
    initSession(filePath)
    val record = fetchSession(filePath)
    // Abort the multipart out-of-band; the DB row stays behind (simulates a crash between
    // LakeFS abort and row deletion, or a previous partially-failed cleanup round).
    LakeFSStorageClient.abortPresignedMultipartUploads(
      repoName,
      filePath,
      record.getUploadId,
      record.getPhysicalAddress
    )
    fetchSession(filePath) should not be null

    val report = job.runCleanupOnce(farFuture)

    report.sessionsDeleted shouldEqual 1
    report.errors shouldEqual 0
    fetchSession(filePath) shouldBe null
  }

  // ===========================================================================
  // 11. Per-item failures are counted and never abort the batch
  // ===========================================================================
  it should "count per-item failures in errors without aborting the rest of the batch" in {
    // A session whose LakeFS multipart cannot be aborted (bogus uploadId / physical address).
    val bogusPath = uniquePath("bogus-session")
    insertBogusSession(bogusPath) // created_at defaults to now -> expired under farFuture

    // A real expired session and an expired staged object that MUST still be cleaned.
    val realExpiredPath = uniquePath("real-expired-session")
    initSession(realExpiredPath)
    val expiredStaged = uniquePath("error-batch-staged")
    stageObject(expiredStaged)

    val report = job.runCleanupOnce(farFuture)

    report.errors should be >= 1
    // The failure must not stop the rest of the batch:
    fetchSession(realExpiredPath) shouldBe null
    uncommittedPaths() should not contain expiredStaged
    report.sessionsDeleted should be >= 1
  }

  // ===========================================================================
  // 12. Lifecycle: stop() before start() is a no-op (executor == null guard)
  // ===========================================================================
  "StagedFileCleanupJob lifecycle" should "allow stop() before start() without throwing" in {
    val lifecycleJob = new StagedFileCleanupJob(RetentionHours, IntervalMinutes)
    noException should be thrownBy lifecycleJob.stop()
  }

  // ===========================================================================
  // 13. Lifecycle: start() then stop() schedules and tears down cleanly
  // ===========================================================================
  it should "start() then stop() without throwing" in {
    // The scheduled task has a 1-minute initial delay, so its body never runs during this
    // test; we are only covering the scheduling + teardown lines, not the lambda.
    val lifecycleJob = new StagedFileCleanupJob(RetentionHours, IntervalMinutes)
    try {
      noException should be thrownBy lifecycleJob.start()
    } finally {
      // Always stop so a started daemon executor never leaks between tests.
      lifecycleJob.stop()
    }
  }

  // ===========================================================================
  // 14. Path 1 (session cleanup): orphan session (dataset has NULL repository_name) — cleaned, no abort attempted
  // ===========================================================================
  it should "delete an orphan session whose dataset has a NULL repository_name, without error" in {
    // A second dataset with NO repository_name: such a did never appears in repoNameByDid,
    // so the cleanup hits the `case None` branch (no multipart abort) and still deletes the row.
    val nullRepoDataset = new Dataset
    nullRepoDataset.setName(s"null-repo-ds-${System.nanoTime()}")
    nullRepoDataset.setRepositoryName(null)
    nullRepoDataset.setIsPublic(true)
    nullRepoDataset.setIsDownloadable(true)
    nullRepoDataset.setDescription("dataset with no LakeFS repo for orphan-session test")
    nullRepoDataset.setOwnerUid(ownerUser.getUid)
    val datasetDao = new DatasetDao(getDSLContext.configuration())
    datasetDao.insert(nullRepoDataset)

    try {
      val orphanUploadId = s"orphan-upload-${System.nanoTime()}"
      getDSLContext
        .insertInto(DATASET_UPLOAD_SESSION)
        .set(DATASET_UPLOAD_SESSION.DID, nullRepoDataset.getDid)
        .set(DATASET_UPLOAD_SESSION.UID, ownerUser.getUid)
        .set(DATASET_UPLOAD_SESSION.FILE_PATH, "orphan/file.bin")
        .set(DATASET_UPLOAD_SESSION.UPLOAD_ID, orphanUploadId)
        .set(DATASET_UPLOAD_SESSION.PHYSICAL_ADDRESS, "s3://whatever/orphan")
        .set(DATASET_UPLOAD_SESSION.NUM_PARTS_REQUESTED, Int.box(1))
        .set(DATASET_UPLOAD_SESSION.FILE_SIZE_BYTES, java.lang.Long.valueOf(16L))
        .set(DATASET_UPLOAD_SESSION.PART_SIZE_BYTES, java.lang.Long.valueOf(32L))
        .execute()

      val report = job.runCleanupOnce(farFuture)

      report.sessionsDeleted shouldEqual 1
      report.errors shouldEqual 0
      getDSLContext
        .selectCount()
        .from(DATASET_UPLOAD_SESSION)
        .where(DATASET_UPLOAD_SESSION.UPLOAD_ID.eq(orphanUploadId))
        .fetchOne(0, classOf[Int]) shouldEqual 0
    } finally {
      // Remove the extra dataset so later tests' repoNameByDid scan / counts are unaffected.
      // (Its session row, if any survived, cascades away with the dataset.)
      datasetDao.deleteById(nullRepoDataset.getDid)
    }
  }

  // ===========================================================================
  // 15. Path 2 (staged objects): staged DELETION of a committed object is skipped (not reset, not an error)
  // ===========================================================================
  it should "skip a staged deletion (REMOVED diff) without resetting it or counting an error" in {
    // Commit an object, then stage a deletion of it on main WITHOUT committing. The pending
    // deletion surfaces as a Diff of type REMOVED, which has no object behind it.
    val committedThenDeleted = uniquePath("staged-deletion")
    stageObject(committedThenDeleted, content = "to-be-deleted")
    LakeFSStorageClient.createCommit(repoName, "main", "commit object for staged-deletion test")

    // Stage the deletion (deleteObject targets the main branch but does not commit).
    LakeFSStorageClient.deleteObject(repoName, committedThenDeleted)

    // Sanity: the pending change is a REMOVED diff for this path.
    val uncommitted = LakeFSStorageClient.retrieveUncommittedObjects(repoName)
    uncommitted.map(_.getPath) should contain(committedThenDeleted)

    val report = job.runCleanupOnce(farFuture)

    // The REMOVED entry is skipped: not counted in objectsReset and not an error.
    report.objectsReset shouldEqual 0
    report.errors shouldEqual 0
    // The staged deletion is left intact (still pending, not reverted by cleanup).
    LakeFSStorageClient
      .retrieveUncommittedObjects(repoName)
      .map(_.getPath) should contain(committedThenDeleted)
  }

  // ===========================================================================
  // 16. Path 2 (staged objects): a dataset pointing at a non-existent LakeFS repo (404) is skipped, no error
  // ===========================================================================
  it should "skip a dataset whose LakeFS repository does not exist, without error" in {
    // A dataset row whose repository was never created in LakeFS. retrieveUncommittedObjects
    // throws ApiException 404, which the job catches and skips.
    val ghostDataset = new Dataset
    ghostDataset.setName(s"ghost-ds-${System.nanoTime()}")
    ghostDataset.setRepositoryName(s"ghost-repo-${System.nanoTime()}")
    ghostDataset.setIsPublic(true)
    ghostDataset.setIsDownloadable(true)
    ghostDataset.setDescription("dataset pointing at a non-existent LakeFS repo")
    ghostDataset.setOwnerUid(ownerUser.getUid)
    val datasetDao = new DatasetDao(getDSLContext.configuration())
    datasetDao.insert(ghostDataset)

    try {
      val report = job.runCleanupOnce(farFuture)
      report.errors shouldEqual 0
    } finally {
      // Remove the ghost dataset so the per-test clean-slate teardown (which only resets the
      // suite repo) and later tests' repo scan are not affected by a repo that doesn't exist.
      datasetDao.deleteById(ghostDataset.getDid)
    }
  }

  // ===========================================================================
  // 17. Path 2 (staged objects): a staged CHANGED diff (content modification) is reset, not skipped
  // ===========================================================================
  it should "reset a staged content change (CHANGED diff) while keeping the committed version" in {
    // Commit content A, then re-upload content B at the SAME path without committing. The
    // pending modification surfaces as a Diff of type CHANGED (the `|| CHANGED` half of
    // isObjectWrite), which must be treated as an object write and reset.
    val changedPath = uniquePath("staged-changed")
    stageObject(changedPath, content = "content-A")
    LakeFSStorageClient.createCommit(repoName, "main", "commit content-A for CHANGED test")

    stageObject(changedPath, content = "content-B-modified")

    // Sanity: the path is now uncommitted (a CHANGED diff, not ADDED, since it was committed).
    LakeFSStorageClient
      .retrieveUncommittedObjects(repoName)
      .find(_.getPath == changedPath)
      .map(_.getType) shouldEqual Some(io.lakefs.clients.sdk.model.Diff.TypeEnum.CHANGED)

    val report = job.runCleanupOnce(farFuture)

    report.objectsReset should be >= 1
    report.errors shouldEqual 0
    // The staged change is reverted...
    uncommittedPaths() should not contain changedPath
    // ...and the committed version (content A) is intact and retrievable.
    committedPaths() should contain(changedPath)
    val committed = LakeFSStorageClient.getFileFromRepo(repoName, "main", changedPath)
    new String(
      java.nio.file.Files.readAllBytes(committed.toPath),
      StandardCharsets.UTF_8
    ) shouldEqual
      "content-A"
  }

  // ===========================================================================
  // 18. Multiple datasets/repos are cleaned in a single round (path-2 loop > 1 repo)
  // ===========================================================================
  it should "reset expired staged objects across multiple datasets, keyed per-dataset" in {
    // A second dataset with its own LakeFS repo, initialized like the suite's.
    val repo2 = s"cleanup-ds2-${System.nanoTime()}"
    val dataset2 = new Dataset
    dataset2.setName(s"cleanup-ds2-${System.nanoTime()}")
    dataset2.setRepositoryName(repo2)
    dataset2.setIsPublic(true)
    dataset2.setIsDownloadable(true)
    dataset2.setDescription("second dataset for multi-repo cleanup test")
    dataset2.setOwnerUid(ownerUser.getUid)
    val datasetDao = new DatasetDao(getDSLContext.configuration())
    datasetDao.insert(dataset2)

    try {
      try LakeFSStorageClient.initRepo(repo2)
      catch {
        case e: ApiException if e.getCode == 409 => // already exists, fine
      }

      val now = farFuture

      // One expired staged object in each repo -> both must be reset.
      val expired1 = uniquePath("multi-expired-1")
      stageObject(expired1) // suite repo
      val expired2 = "multi-expired-2/obj.bin"
      LakeFSStorageClient.writeFileToRepo(
        repo2,
        expired2,
        new ByteArrayInputStream("staged-bytes".getBytes(StandardCharsets.UTF_8))
      )

      // An active session in dataset1 (path P) must NOT protect a same-named path in dataset2.
      // Stage P in repo2 with no active session there.
      val sharedPath = "shared/same-path.bin"
      val activeUploadId = initSession(sharedPath) // active session for dataset1 only
      setSessionCreatedAt(activeUploadId, now.minusMinutes(5)) // fresh relative to `now`
      stageObject(sharedPath) // staged in dataset1 -> protected
      LakeFSStorageClient.writeFileToRepo(
        repo2,
        sharedPath,
        new ByteArrayInputStream("staged-bytes".getBytes(StandardCharsets.UTF_8))
      ) // staged in dataset2 -> NOT protected (no session for dataset2)

      val report = job.runCleanupOnce(now)

      // Reset: expired1 (repo1), expired2 (repo2), and sharedPath in repo2 = 3.
      report.objectsReset shouldEqual 3
      report.errors shouldEqual 0

      // dataset1: expired object gone, but the active-session-protected path survives.
      uncommittedPaths() should not contain expired1
      uncommittedPaths() should contain(sharedPath)

      // dataset2: both staged objects gone (the dataset1 active path did not protect them).
      val repo2Uncommitted = LakeFSStorageClient.retrieveUncommittedObjects(repo2).map(_.getPath)
      repo2Uncommitted should not contain expired2
      repo2Uncommitted should not contain sharedPath
    } finally {
      // Drop the extra dataset + repo so the suite's single-dataset assumptions hold and the
      // per-test clean-slate (which only resets the suite repo) isn't affected.
      datasetDao.deleteById(dataset2.getDid)
      try LakeFSStorageClient.deleteRepo(repo2)
      catch { case _: ApiException => /* best-effort cleanup */ }
    }
  }

  // ===========================================================================
  // 19. Path 1 (session cleanup): a non-404 abort failure rolls back the row delete (transactional)
  // ===========================================================================
  it should "roll back the session-row delete when the multipart abort fails (non-404)" in {
    // A bogus uploadId / physical address makes the multipart abort throw a NON-404 error.
    // The DB row must SURVIVE, proving the delete (staged first inside withTransaction) is
    // rolled back rather than committed. A timeout or a 5xx from LakeFS takes this same
    // non-404 -> rollback -> retry path.
    val bogusPath = uniquePath("rollback-bogus-session")
    insertBogusSession(bogusPath) // created_at defaults to now -> expired under farFuture
    val bogusId = fetchSession(bogusPath).getUploadId

    try {
      val report = job.runCleanupOnce(farFuture)

      report.errors shouldEqual 1
      report.sessionsDeleted shouldEqual 0
      // The delete was rolled back together with the failed abort, so the row is still
      // present and the next round will retry it.
      fetchSession(bogusPath) should not be null
    } finally {
      // The row survives the rolled-back round, so remove it explicitly to keep later tests'
      // exact counts independent (beforeEach also clears the table, but be explicit).
      getDSLContext
        .deleteFrom(DATASET_UPLOAD_SESSION)
        .where(DATASET_UPLOAD_SESSION.UPLOAD_ID.eq(bogusId))
        .execute()
    }
  }

  // ===========================================================================
  // 20. Path 1 (session cleanup): a transiently-failing session is cleaned on the NEXT round (self-heals)
  // ===========================================================================
  it should "clean a transiently-failing session on the next round (retried, not stuck)" in {
    // Round 1's abort fails (non-404) and rolls back, leaving the row; once the transient
    // condition clears, a later round succeeds, so the row is retried rather than stuck.
    val filePath = uniquePath("transient-session")
    insertBogusSession(filePath) // abort throws non-404 under the bogus physical address
    val bogusId = fetchSession(filePath).getUploadId

    try {
      // Round 1: abort fails -> transaction rolls back -> row survives, counted as an error.
      val round1 = job.runCleanupOnce(farFuture)
      round1.errors shouldEqual 1
      round1.sessionsDeleted shouldEqual 0
      fetchSession(filePath) should not be null

      // Clear the transient failure deterministically WITHOUT faking the client: replace the
      // bogus row with a REAL session at the same logical path, then abort its multipart
      // out-of-band. The next round's abort therefore returns 404,
      // which the job treats as success.
      getDSLContext
        .deleteFrom(DATASET_UPLOAD_SESSION)
        .where(DATASET_UPLOAD_SESSION.UPLOAD_ID.eq(bogusId))
        .execute()
      initSession(filePath)
      val healed = fetchSession(filePath)
      LakeFSStorageClient.abortPresignedMultipartUploads(
        repoName,
        filePath,
        healed.getUploadId,
        healed.getPhysicalAddress
      )

      // Round 2: abort hits the already-aborted 404 (success) -> row is deleted, no error.
      val round2 = job.runCleanupOnce(farFuture)
      round2.errors shouldEqual 0
      round2.sessionsDeleted shouldEqual 1
      fetchSession(filePath) shouldBe null
    } finally {
      // Best-effort: remove any row that may survive an unexpected mid-test failure.
      getDSLContext
        .deleteFrom(DATASET_UPLOAD_SESSION)
        .where(DATASET_UPLOAD_SESSION.FILE_PATH.eq(filePath))
        .execute()
    }
  }

  // ===========================================================================
  // 21. Path 1 (session cleanup): a failing item does not prevent a healthy item in the same round from cleaning
  // ===========================================================================
  it should "clean a healthy session in the same round where another item fails, keeping the failed row" in {
    // Verifies both halves in a single round: a healthy item is cleaned and the batch
    // continues, while the failing item's row is rolled back (survives) to retry next round.
    val healthyPath = uniquePath("healthy-alongside-failing")
    initSession(healthyPath) // abort succeeds -> row deleted
    val bogusPath = uniquePath("failing-alongside-healthy")
    insertBogusSession(bogusPath) // abort throws non-404 -> transaction rolls back
    val bogusId = fetchSession(bogusPath).getUploadId

    try {
      val report = job.runCleanupOnce(farFuture)

      report.sessionsDeleted shouldEqual 1
      report.errors shouldEqual 1
      // Healthy item cleaned despite the sibling failure...
      fetchSession(healthyPath) shouldBe null
      // ...and the failing item's row is rolled back (survives), ready to retry next round.
      fetchSession(bogusPath) should not be null
    } finally {
      getDSLContext
        .deleteFrom(DATASET_UPLOAD_SESSION)
        .where(DATASET_UPLOAD_SESSION.UPLOAD_ID.eq(bogusId))
        .execute()
    }
  }

  // ===========================================================================
  // 22. TOCTOU guard: the mtime is re-read right before reset; a now-fresh object is skipped
  // ===========================================================================
  it should "skip reset when the mtime re-read just before reset is fresh (TOCTOU guard)" in {
    val stagedPath = uniquePath("toctou-skip")
    stageObject(stagedPath)
    val now = farFuture
    val cutoffEpoch = now.minusHours(RetentionHours.toLong).toEpochSecond

    // First read looks expired (old), but the re-read taken right before the reset sees a fresh
    // mtime, as if a new upload landed on the same path between the two reads. The object must
    // NOT be reset, which is exactly the data-loss window the second read closes.
    val calls = new java.util.concurrent.atomic.AtomicInteger(0)
    val mtimeOf: (String, String) => Long =
      (_, _) => if (calls.incrementAndGet() == 1) cutoffEpoch - 100 else cutoffEpoch + 100

    val report = job.runCleanupOnce(now, mtimeOf)

    report.objectsReset shouldEqual 0
    report.errors shouldEqual 0
    calls.get() shouldEqual 2 // both the initial read and the pre-reset re-read happened
    uncommittedPaths() should contain(stagedPath)
  }

  // ===========================================================================
  // 23. TOCTOU guard: when the re-read is still expired, the object is reset (pass-through)
  // ===========================================================================
  it should "reset when the mtime re-read just before reset is still expired" in {
    val stagedPath = uniquePath("toctou-reset")
    stageObject(stagedPath)
    val now = farFuture
    val cutoffEpoch = now.minusHours(RetentionHours.toLong).toEpochSecond

    val report = job.runCleanupOnce(now, (_, _) => cutoffEpoch - 100)

    report.objectsReset should be >= 1
    report.errors shouldEqual 0
    uncommittedPaths() should not contain stagedPath
  }

  // ===========================================================================
  // 24. Scheduled tick delegates to runCleanupOnce on each invocation
  // ===========================================================================
  "StagedFileCleanupJob scheduled tick" should "run a cleanup round on each invocation" in {
    val counter = new java.util.concurrent.atomic.AtomicInteger(0)
    val countingJob = new StagedFileCleanupJob(RetentionHours, IntervalMinutes) {
      override def runCleanupOnce(
          now: OffsetDateTime,
          mtimeOf: (String, String) => Long
      ): CleanupReport = {
        counter.incrementAndGet()
        CleanupReport(0, 0, 0)
      }
    }

    countingJob.runScheduledTick()
    countingJob.runScheduledTick()

    counter.get() shouldEqual 2
  }

  // ===========================================================================
  // 25. Scheduled tick swallows exceptions so the fixed-delay schedule is never cancelled
  // ===========================================================================
  it should "swallow exceptions from a cleanup round so the schedule keeps running" in {
    val throwingJob = new StagedFileCleanupJob(RetentionHours, IntervalMinutes) {
      override def runCleanupOnce(
          now: OffsetDateTime,
          mtimeOf: (String, String) => Long
      ): CleanupReport = throw new RuntimeException("boom")
    }

    noException should be thrownBy throwingJob.runScheduledTick()
  }

  // ===========================================================================
  // 26. Constructor rejects non-positive configuration
  // ===========================================================================
  "StagedFileCleanupJob constructor" should "reject a non-positive retentionHours" in {
    assertThrows[IllegalArgumentException](new StagedFileCleanupJob(0, IntervalMinutes))
  }

  it should "reject a non-positive intervalMinutes" in {
    assertThrows[IllegalArgumentException](new StagedFileCleanupJob(RetentionHours, 0))
  }

  it should "reject a non-positive sessionCleanupBatchSize" in {
    assertThrows[IllegalArgumentException](
      new StagedFileCleanupJob(RetentionHours, IntervalMinutes, sessionCleanupBatchSize = 0)
    )
  }
}
