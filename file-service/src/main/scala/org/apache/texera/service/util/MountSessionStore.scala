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

import java.security.SecureRandom
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
  * A short-lived credential that lets a computing-unit pod's GeeseFS mount reach the
  * LakeFS S3 gateway through [[S3ProxyServlet]] without holding any global LakeFS
  * credentials. A session is scoped to exactly one dataset version (`repositoryName`
  * at `commitHash`); the proxy rejects any request outside that scope.
  */
case class MountSession(
    accessKey: String,
    secretKey: String,
    repositoryName: String,
    commitHash: String,
    uid: Integer,
    expiresAtEpochMs: Long
)

/**
  * In-memory store of active mount sessions, keyed by the session access key that the
  * pod presents to the proxy. file-service runs a single replica, so an in-process map
  * is sufficient; sessions expire on a sliding window so a long-running, actively-read
  * mount stays valid while idle ones are reclaimed.
  */
object MountSessionStore {

  // Sliding-window lifetime: a session stays valid as long as it is used at least this
  // often. Comfortably longer than a scheduling gap between reads of a mounted dataset.
  val TtlMillis: Long = 6L * 60 * 60 * 1000 // 6 hours

  private val sessions = new ConcurrentHashMap[String, MountSession]()
  private val random = new SecureRandom()

  private def randomToken(numBytes: Int): String = {
    val buf = new Array[Byte](numBytes)
    random.nextBytes(buf)
    Base64.getUrlEncoder.withoutPadding.encodeToString(buf)
  }

  /**
    * Create and register a session scoped to `repositoryName`@`commitHash` for `uid`.
    */
  def create(
      repositoryName: String,
      commitHash: String,
      uid: Integer,
      nowMs: Long
  ): MountSession = {
    purgeExpired(nowMs)
    // The access key is what the pod passes to GeeseFS as AWS_ACCESS_KEY_ID and what the
    // proxy reads back out of the SigV4 Credential; the prefix makes it recognizable in logs.
    val session = MountSession(
      accessKey = "TXMNT" + randomToken(18),
      secretKey = randomToken(30),
      repositoryName = repositoryName,
      commitHash = commitHash,
      uid = uid,
      expiresAtEpochMs = nowMs + TtlMillis
    )
    sessions.put(session.accessKey, session)
    session
  }

  /**
    * Look up a live session by access key, extending its lifetime (sliding window).
    * Returns None if unknown or expired.
    */
  def get(accessKey: String, nowMs: Long): Option[MountSession] = {
    Option(sessions.get(accessKey)).flatMap { s =>
      if (s.expiresAtEpochMs < nowMs) {
        sessions.remove(accessKey)
        None
      } else {
        val renewed = s.copy(expiresAtEpochMs = nowMs + TtlMillis)
        sessions.put(accessKey, renewed)
        Some(renewed)
      }
    }
  }

  private def purgeExpired(nowMs: Long): Unit = {
    sessions
      .entrySet()
      .asScala
      .filter(_.getValue.expiresAtEpochMs < nowMs)
      .foreach(e => sessions.remove(e.getKey))
  }
}
