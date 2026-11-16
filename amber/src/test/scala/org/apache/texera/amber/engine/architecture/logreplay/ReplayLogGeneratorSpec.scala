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

package org.apache.texera.amber.engine.architecture.logreplay

import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.engine.common.AmberSerdeTestSupport
import org.apache.texera.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import org.apache.texera.amber.engine.common.storage.{SequentialRecordStorage, VFSRecordStorage}
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.{Files, Path}

class ReplayLogGeneratorSpec extends AnyFlatSpec with AmberSerdeTestSupport {

  override protected def restorePriorAmberRuntime: Boolean = true

  private def cleanup(sub: Path): Unit =
    deleteRecursively(sub.getParent, ignoreWindowsFsException = true)

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  private val cid: ChannelIdentity =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)
  private val destA: EmbeddedControlMessageIdentity = EmbeddedControlMessageIdentity("dest-A")
  private val destB: EmbeddedControlMessageIdentity = EmbeddedControlMessageIdentity("dest-B")

  private def newStorage(): (Path, SequentialRecordStorage[ReplayLogRecord]) = {
    val root = Files.createTempDirectory("replay-log-generator-spec-")
    val sub = root.resolve("logs")
    val storage = new VFSRecordStorage[ReplayLogRecord](sub.toUri)
    (sub, storage)
  }

  private def writeLog(
      storage: SequentialRecordStorage[ReplayLogRecord],
      records: Seq[ReplayLogRecord]
  ): Unit = {
    val writer = storage.getWriter("log")
    // Close in a finally so a serialization failure mid-write does not leak
    // the underlying output stream (which would otherwise block temp-dir
    // cleanup, especially on Windows).
    try {
      records.foreach(writer.writeRecord)
      writer.flush()
    } finally {
      writer.close()
    }
  }

  private def msg(seq: Long): WorkflowFIFOMessage =
    WorkflowFIFOMessage(cid, seq, DataFrame(Array.empty))

  // ---------------------------------------------------------------------------
  // Empty storage
  // ---------------------------------------------------------------------------

  "ReplayLogGenerator.generate" should
    "return empty queues when the storage is an EmptyRecordStorage" in {
    val storage = SequentialRecordStorage.getStorage[ReplayLogRecord](None)
    val (steps, messages) = ReplayLogGenerator.generate(storage, "log", destA)
    assert(steps.isEmpty)
    assert(messages.isEmpty)
  }

  it should "return empty queues when the storage file exists but holds no records" in {
    val (sub, storage) = newStorage()
    try {
      writeLog(storage, Seq.empty)
      val (steps, messages) = ReplayLogGenerator.generate(storage, "log", destA)
      assert(steps.isEmpty)
      assert(messages.isEmpty)
    } finally {
      cleanup(sub)
    }
  }

  // ---------------------------------------------------------------------------
  // Partitioning by record type
  // ---------------------------------------------------------------------------

  it should "enqueue all ProcessingStep records into the steps queue (preserving order)" in {
    val (sub, storage) = newStorage()
    try {
      val recs = Seq(
        ProcessingStep(cid, 1L),
        ProcessingStep(cid, 2L),
        ProcessingStep(cid, 3L)
      )
      writeLog(storage, recs)
      val (steps, messages) = ReplayLogGenerator.generate(storage, "log", destA)
      assert(steps.toList == recs)
      assert(messages.isEmpty)
    } finally {
      cleanup(sub)
    }
  }

  it should "enqueue all MessageContent records into the messages queue (preserving order)" in {
    val (sub, storage) = newStorage()
    try {
      val m1 = msg(1L)
      val m2 = msg(2L)
      writeLog(storage, Seq(MessageContent(m1), MessageContent(m2)))
      val (steps, messages) = ReplayLogGenerator.generate(storage, "log", destA)
      assert(steps.isEmpty)
      // The pair is round-tripped through Kryo; the message reference is
      // not preserved (a fresh deserialized copy comes back) — so compare
      // by case-class equality, not `eq`.
      assert(messages.toList == List(m1, m2))
    } finally {
      cleanup(sub)
    }
  }

  it should
    "partition steps and messages independently when records are interleaved" in {
    val (sub, storage) = newStorage()
    try {
      val s1 = ProcessingStep(cid, 1L)
      val s2 = ProcessingStep(cid, 2L)
      val m1 = msg(1L)
      val m2 = msg(2L)
      writeLog(
        storage,
        Seq(s1, MessageContent(m1), s2, MessageContent(m2))
      )
      val (steps, messages) = ReplayLogGenerator.generate(storage, "log", destA)
      assert(steps.toList == List(s1, s2))
      assert(messages.toList == List(m1, m2))
    } finally {
      cleanup(sub)
    }
  }

  // ---------------------------------------------------------------------------
  // ReplayDestination — early termination + skip semantics
  // ---------------------------------------------------------------------------

  it should
    "short-circuit at the matching ReplayDestination, ignoring records that follow it" in {
    val (sub, storage) = newStorage()
    try {
      val s1 = ProcessingStep(cid, 1L)
      val m1 = msg(1L)
      val m2Past = msg(2L) // must NOT appear in the result
      val s2Past = ProcessingStep(cid, 99L) // must NOT appear either
      writeLog(
        storage,
        Seq(
          s1,
          MessageContent(m1),
          ReplayDestination(destA), // <-- replayTo target; iteration stops here
          MessageContent(m2Past),
          s2Past
        )
      )
      val (steps, messages) = ReplayLogGenerator.generate(storage, "log", destA)
      assert(steps.toList == List(s1))
      assert(messages.toList == List(m1))
    } finally {
      cleanup(sub)
    }
  }

  it should
    "silently skip a ReplayDestination whose id does not match replayTo (iteration continues)" in {
    val (sub, storage) = newStorage()
    try {
      val s1 = ProcessingStep(cid, 1L)
      val m1 = msg(1L)
      writeLog(
        storage,
        Seq(
          s1,
          ReplayDestination(destB), // <-- different id; skipped
          MessageContent(m1)
        )
      )
      val (steps, messages) = ReplayLogGenerator.generate(storage, "log", destA)
      assert(steps.toList == List(s1))
      assert(messages.toList == List(m1))
    } finally {
      cleanup(sub)
    }
  }

  it should
    "stop at the FIRST matching ReplayDestination when multiple matching records exist" in {
    val (sub, storage) = newStorage()
    try {
      val s1 = ProcessingStep(cid, 1L)
      val s2Past = ProcessingStep(cid, 2L)
      writeLog(
        storage,
        Seq(
          s1,
          ReplayDestination(destA), // <-- first match
          s2Past, // <-- after the cut
          ReplayDestination(destA)
        )
      )
      val (steps, _) = ReplayLogGenerator.generate(storage, "log", destA)
      assert(steps.toList == List(s1))
    } finally {
      cleanup(sub)
    }
  }

  // ---------------------------------------------------------------------------
  // Unknown record type — TerminateSignal triggers the `case other` branch
  // ---------------------------------------------------------------------------

  it should "throw RuntimeException on an unhandled record type (e.g. TerminateSignal)" in {
    val (sub, storage) = newStorage()
    try {
      writeLog(storage, Seq(TerminateSignal))
      val ex = intercept[RuntimeException] {
        ReplayLogGenerator.generate(storage, "log", destA)
      }
      assert(
        ex.getMessage.toLowerCase.contains("cannot handle"),
        s"expected diagnostic message about unhandled record, got: ${ex.getMessage}"
      )
    } finally {
      cleanup(sub)
    }
  }

  // ---------------------------------------------------------------------------
  // Mixed-record full-cycle
  // ---------------------------------------------------------------------------

  it should "handle a realistic mix of steps + messages + non-matching destinations" in {
    val (sub, storage) = newStorage()
    try {
      val s1 = ProcessingStep(cid, 10L)
      val s2 = ProcessingStep(cid, 20L)
      val s3 = ProcessingStep(cid, 30L)
      val m1 = msg(1L)
      val m2 = msg(2L)
      writeLog(
        storage,
        Seq(
          s1,
          MessageContent(m1),
          ReplayDestination(destB), // skipped (id mismatch)
          s2,
          MessageContent(m2),
          ReplayDestination(destB), // also skipped
          s3
        )
      )
      val (steps, messages) = ReplayLogGenerator.generate(storage, "log", destA)
      assert(steps.toList == List(s1, s2, s3))
      assert(messages.toList == List(m1, m2))
    } finally {
      cleanup(sub)
    }
  }
}
