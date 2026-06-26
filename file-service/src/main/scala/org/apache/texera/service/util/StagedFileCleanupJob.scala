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

import com.typesafe.scalalogging.LazyLogging
import io.dropwizard.lifecycle.Managed
import io.lakefs.clients.sdk.ApiException
import io.lakefs.clients.sdk.model.Diff
import org.apache.texera.amber.core.storage.util.LakeFSStorageClient
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.tables.Dataset.DATASET
import org.apache.texera.dao.jooq.generated.tables.DatasetUploadSession.DATASET_UPLOAD_SESSION
import org.jooq.DSLContext

import java.time.OffsetDateTime
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.jdk.CollectionConverters._

/**
  * Summary of one cleanup round.
  *
  * @param sessionsDeleted Number of abandoned upload session rows deleted.
  * @param objectsReset    Number of staged (uncommitted) objects reset in LakeFS.
  * @param errors          Number of failures encountered (each is retried next round).
  */
case class CleanupReport(sessionsDeleted: Int, objectsReset: Int, errors: Int)

object StagedFileCleanupJob {
  private[util] val DefaultSessionCleanupBatchSize = 500
}

/**
  * Periodically cleans up uploaded but uncommitted dataset files:
  *   1. Aborts and deletes abandoned multipart upload sessions older than the retention window.
  *   2. Resets staged (uncommitted) LakeFS objects older than the retention window, skipping
  *      objects that belong to still-active upload sessions.
  *
  * @param retentionHours  Age (in hours) after which uncommitted uploads are cleaned up.
  * @param intervalMinutes Delay (in minutes) between cleanup rounds.
  */
class StagedFileCleanupJob(
    retentionHours: Int,
    intervalMinutes: Int,
    sessionCleanupBatchSize: Int = StagedFileCleanupJob.DefaultSessionCleanupBatchSize
) extends Managed
    with LazyLogging {

  require(retentionHours > 0, s"retentionHours must be > 0 (got $retentionHours)")
  require(intervalMinutes > 0, s"intervalMinutes must be > 0 (got $intervalMinutes)")
  require(
    sessionCleanupBatchSize > 0,
    s"sessionCleanupBatchSize must be > 0 (got $sessionCleanupBatchSize)"
  )

  private var executor: ScheduledExecutorService = _

  override def start(): Unit = {
    executor = Executors.newSingleThreadScheduledExecutor((runnable: Runnable) => {
      val thread = new Thread(runnable, "staged-file-cleanup")
      thread.setDaemon(true)
      thread
    })
    executor.scheduleWithFixedDelay(
      () => runScheduledTick(),
      // Small fixed initial delay so a restart doesn't postpone backlog cleanup by up to a
      // full interval.
      1L,
      intervalMinutes.toLong,
      TimeUnit.MINUTES
    )
  }

  /**
    * Runs one cleanup round for the scheduler. Visible for testing. Catches every Throwable
    * because an exception escaping the scheduled task would cancel the fixed-delay schedule and
    * silently stop all future cleanup rounds.
    */
  private[util] def runScheduledTick(): Unit =
    try {
      runCleanupOnce()
    } catch {
      case t: Throwable => logger.error("Staged file cleanup round failed", t)
    }

  override def stop(): Unit = {
    if (executor != null) {
      executor.shutdown()
    }
  }

  /**
    * Runs a single cleanup round. Idempotent: rows/objects already cleaned up are not
    * revisited, and failures are retried on the next round.
    *
    * @param now The reference time used to evaluate the retention window.
    * @return Summary counts for this round.
    */
  private[util] def runCleanupOnce(
      now: OffsetDateTime = OffsetDateTime.now(),
      mtimeOf: (String, String) => Long = LakeFSStorageClient.getStagedObjectMtime
  ): CleanupReport = {
    val cutoff = now.minusHours(retentionHours.toLong)
    val cutoffEpochSecond = cutoff.toEpochSecond
    var sessionsDeleted = 0
    var objectsReset = 0
    var errors = 0

    val ctx = SqlServer.getInstance().createDSLContext()

    // Map each dataset id to its LakeFS repository name (same mapping DatasetResource uses
    // via dataset.getRepositoryName).
    val repoNameByDid: Map[Integer, String] = ctx
      .select(DATASET.DID, DATASET.REPOSITORY_NAME)
      .from(DATASET)
      .where(DATASET.REPOSITORY_NAME.isNotNull)
      .fetch()
      .asScala
      .map(record => record.get(DATASET.DID) -> record.get(DATASET.REPOSITORY_NAME))
      .toMap

    // Path 1: abort and delete abandoned multipart upload sessions.
    val expiredSessions = ctx
      .selectFrom(DATASET_UPLOAD_SESSION)
      .where(DATASET_UPLOAD_SESSION.CREATED_AT.lt(cutoff))
      .orderBy(DATASET_UPLOAD_SESSION.CREATED_AT.asc())
      .limit(sessionCleanupBatchSize)
      .fetch()
      .asScala
      .toList

    expiredSessions.foreach { session =>
      try {
        // Delete the row and abort the multipart in one transaction, deleting FIRST. LakeFS is
        // external and cannot truly enroll in a DB transaction, but the abort is idempotent
        // (re-aborting an already-aborted upload returns 404, treated as success below), so the
        // only risk is the abort failing AFTER the delete is staged. By staging the delete first
        // and letting a non-404 abort failure roll the whole transaction back, the session row
        // survives and the next round retries — never leaving an orphaned multipart behind.
        SqlServer.withTransaction(ctx) { txn =>
          txn
            .deleteFrom(DATASET_UPLOAD_SESSION)
            .where(DATASET_UPLOAD_SESSION.UPLOAD_ID.eq(session.getUploadId))
            .execute()
          repoNameByDid.get(session.getDid) match {
            case Some(repoName) =>
              try {
                LakeFSStorageClient.abortPresignedMultipartUploads(
                  repoName,
                  session.getFilePath,
                  session.getUploadId,
                  session.getPhysicalAddress
                )
              } catch {
                // Already aborted (or never materialized): safe to delete the session row.
                case e: ApiException if e.getCode == 404 =>
                  logger.debug(
                    s"Multipart upload ${session.getUploadId} not found in LakeFS; " +
                      "treating as already aborted"
                  )
              }
            case None =>
              // Dataset row gone or repository_name is NULL: the multipart lived in that
              // repository's namespace, so there is nothing left to abort.
              logger.debug(
                s"No repository for dataset ${session.getDid}; " +
                  s"deleting orphan upload session ${session.getUploadId}"
              )
          }
        }
        sessionsDeleted += 1
      } catch {
        case t: Throwable =>
          logger.warn(
            s"Failed to clean up upload session ${session.getUploadId} " +
              s"(did=${session.getDid}, path=${session.getFilePath}); will retry next round",
            t
          )
          errors += 1
      }
    }

    // Path 2: reset staged (uncommitted) objects older than the retention window.
    repoNameByDid.foreach {
      case (did, repoName) =>
        try {
          val stagedObjects = LakeFSStorageClient.retrieveUncommittedObjects(repoName)
          // diffBranch carries no mtime, so old write candidates need statObject calls.
          // Re-check immediately before reset so a new upload to the same path is not judged
          // by stale session/mtime reads from earlier in the cleanup round.
          stagedObjects.foreach { diff =>
            val path = diff.getPath
            val isObjectWrite =
              diff.getType == Diff.TypeEnum.ADDED || diff.getType == Diff.TypeEnum.CHANGED
            if (!isObjectWrite) {
              // E.g. a staged deletion of a committed file: there is no object behind it and
              // it consumes no storage, so leaving it is correct and cheap.
              logger.debug(s"Skipping staged ${diff.getType} entry '$path' in '$repoName'")
            } else {
              try {
                val mtime = mtimeOf(repoName, path)
                if (
                  mtime < cutoffEpochSecond &&
                  !hasActiveUploadSession(ctx, did, path, cutoff)
                ) {
                  val latestMtime = mtimeOf(repoName, path)
                  if (latestMtime < cutoffEpochSecond) {
                    LakeFSStorageClient.resetObjectUploadOrDeletion(repoName, path)
                    objectsReset += 1
                  }
                }
              } catch {
                // Concurrently committed/reset, or already cleaned by another round: the
                // object is gone, which is the desired end state for an idempotent job.
                case e: ApiException if e.getCode == 404 =>
                  logger.debug(
                    s"Staged object '$path' not found in repo '$repoName'; " +
                      "treating as already cleaned"
                  )
                case t: Throwable =>
                  logger.warn(
                    s"Failed to clean up staged object '$path' in repo '$repoName'",
                    t
                  )
                  errors += 1
              }
            }
          }
        } catch {
          // The dataset's LakeFS repository was deleted out-of-band (a supported state):
          // nothing staged to clean up there.
          case e: ApiException if e.getCode == 404 =>
            logger.debug(s"Repository '$repoName' not found in LakeFS; skipping")
          case t: Throwable =>
            logger.warn(s"Failed to clean up staged objects in repo '$repoName'", t)
            errors += 1
        }
    }

    logger.debug(
      s"Staged file cleanup round finished: sessionsDeleted=$sessionsDeleted, " +
        s"objectsReset=$objectsReset, errors=$errors"
    )
    CleanupReport(sessionsDeleted, objectsReset, errors)
  }

  private def hasActiveUploadSession(
      ctx: DSLContext,
      did: Integer,
      path: String,
      cutoff: OffsetDateTime
  ): Boolean =
    ctx.fetchExists(
      ctx
        .selectOne()
        .from(DATASET_UPLOAD_SESSION)
        .where(
          DATASET_UPLOAD_SESSION.DID
            .eq(did)
            .and(DATASET_UPLOAD_SESSION.FILE_PATH.eq(path))
            .and(DATASET_UPLOAD_SESSION.CREATED_AT.ge(cutoff))
        )
    )
}
