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

/**
  * Periodically cleans up uploaded but uncommitted dataset files:
  *   1. Aborts and deletes abandoned multipart upload sessions older than the retention window.
  *   2. Resets staged (uncommitted) LakeFS objects older than the retention window, skipping
  *      objects that belong to still-active upload sessions.
  *
  * @param retentionHours  Age (in hours) after which uncommitted uploads are cleaned up.
  * @param intervalMinutes Delay (in minutes) between cleanup rounds.
  */
class StagedFileCleanupJob(retentionHours: Int, intervalMinutes: Int)
    extends Managed
    with LazyLogging {

  require(retentionHours > 0, s"retentionHours must be > 0 (got $retentionHours)")
  require(intervalMinutes > 0, s"intervalMinutes must be > 0 (got $intervalMinutes)")

  private var executor: ScheduledExecutorService = _

  override def start(): Unit = {
    executor = Executors.newSingleThreadScheduledExecutor((runnable: Runnable) => {
      val thread = new Thread(runnable, "staged-file-cleanup")
      thread.setDaemon(true)
      thread
    })
    executor.scheduleWithFixedDelay(
      () => {
        try {
          runCleanupOnce()
        } catch {
          // An exception must never kill the schedule.
          case t: Throwable => logger.error("Staged file cleanup round failed", t)
        }
      },
      // Small fixed initial delay so a restart doesn't postpone backlog cleanup by up to a
      // full interval.
      1L,
      intervalMinutes.toLong,
      TimeUnit.MINUTES
    )
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
  def runCleanupOnce(now: OffsetDateTime = OffsetDateTime.now()): CleanupReport = {
    val cutoff = now.minusHours(retentionHours.toLong)
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

    // File paths of still-active (non-expired) upload sessions, per dataset. Staged objects
    // belonging to an active upload must never be reset.
    val activePathsByDid: Map[Integer, Set[String]] = ctx
      .select(DATASET_UPLOAD_SESSION.DID, DATASET_UPLOAD_SESSION.FILE_PATH)
      .from(DATASET_UPLOAD_SESSION)
      .where(DATASET_UPLOAD_SESSION.CREATED_AT.ge(cutoff))
      .fetch()
      .asScala
      .groupBy(record => record.get(DATASET_UPLOAD_SESSION.DID))
      .map {
        case (did, records) =>
          did -> records.map(_.get(DATASET_UPLOAD_SESSION.FILE_PATH)).toSet
      }

    // Path 2: reset staged (uncommitted) objects older than the retention window.
    repoNameByDid.foreach {
      case (did, repoName) =>
        try {
          val activePaths = activePathsByDid.getOrElse(did, Set.empty)
          val stagedObjects = LakeFSStorageClient.retrieveUncommittedObjects(repoName)
          // diffBranch carries no mtime, so each candidate costs one extra statObject call
          // (N+1). Unavoidable until LakeFS exposes timestamps in the diff API.
          stagedObjects.foreach { diff =>
            val path = diff.getPath
            val isObjectWrite =
              diff.getType == Diff.TypeEnum.ADDED || diff.getType == Diff.TypeEnum.CHANGED
            if (!isObjectWrite) {
              // E.g. a staged deletion of a committed file: there is no object behind it and
              // it consumes no storage, so leaving it is correct and cheap.
              logger.debug(s"Skipping staged ${diff.getType} entry '$path' in '$repoName'")
            } else if (!activePaths.contains(path)) {
              try {
                val mtime = LakeFSStorageClient.getStagedObjectMtime(repoName, path)
                if (mtime < cutoff.toEpochSecond) {
                  LakeFSStorageClient.resetObjectUploadOrDeletion(repoName, path)
                  objectsReset += 1
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

    logger.info(
      s"Staged file cleanup round finished: sessionsDeleted=$sessionsDeleted, " +
        s"objectsReset=$objectsReset, errors=$errors"
    )
    CleanupReport(sessionsDeleted, objectsReset, errors)
  }
}
