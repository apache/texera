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

import org.apache.commons.io.FileUtils
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.engine.common.AmberSerdeTestSupport
import org.apache.texera.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import org.apache.texera.amber.engine.common.storage.{SequentialRecordStorage, VFSRecordStorage}
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Files

class ReplayLogGeneratorSpec extends AnyFlatSpec with AmberSerdeTestSupport {

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  private val cid: ChannelIdentity =
    ChannelIdentity(ActorVirtualIdentity("from"), ActorVirtualIdentity("to"), isControl = false)
  private val destA: EmbeddedControlMessageIdentity = EmbeddedControlMessageIdentity("dest-A")
  private val destB: EmbeddedControlMessageIdentity = EmbeddedControlMessageIdentity("dest-B")

  private def step(seq: Long): ProcessingStep = ProcessingStep(cid, seq)

  private def msg(seq: Long): WorkflowFIFOMessage =
    WorkflowFIFOMessage(cid, seq, DataFrame(Array.empty))

  private def generateFrom(records: ReplayLogRecord*) = {
    val root = Files.createTempDirectory("replay-log-generator-spec-")
    val sub = root.resolve("logs")
    val storage = new VFSRecordStorage[ReplayLogRecord](sub.toUri)
    val writer = storage.getWriter("log")
    // Close in a finally so a serialization failure mid-write does not leak
    // the underlying output stream (which would otherwise block temp-dir
    // cleanup, especially on Windows).
    try {
      try {
        records.foreach(writer.writeRecord)
        writer.flush()
      } finally {
        writer.close()
      }
      ReplayLogGenerator.generate(storage, "log", destA)
    } finally {
      FileUtils.deleteQuietly(sub.getParent.toFile)
    }
  }

  private def assertGenerated(
      expectedSteps: Seq[ProcessingStep],
      expectedMessages: Seq[WorkflowFIFOMessage]
  )(records: ReplayLogRecord*): Unit = {
    val (steps, messages) = generateFrom(records: _*)
    assert(steps.toList == expectedSteps)
    assert(messages.toList == expectedMessages)
  }

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
    assertGenerated(Nil, Nil)()
  }

  // ---------------------------------------------------------------------------
  // Partitioning by record type
  // ---------------------------------------------------------------------------

  it should "enqueue all ProcessingStep records into the steps queue (preserving order)" in {
    val recs = Seq(step(1L), step(2L), step(3L))
    assertGenerated(recs, Nil)(recs: _*)
  }

  it should "enqueue all MessageContent records into the messages queue (preserving order)" in {
    val (m1, m2) = (msg(1L), msg(2L))
    // The pair is round-tripped through Kryo; the message reference is
    // not preserved (a fresh deserialized copy comes back) — so compare
    // by case-class equality, not `eq`.
    assertGenerated(Nil, Seq(m1, m2))(MessageContent(m1), MessageContent(m2))
  }

  it should
    "partition steps and messages independently when records are interleaved" in {
    val (s1, s2, m1, m2) = (step(1L), step(2L), msg(1L), msg(2L))
    assertGenerated(Seq(s1, s2), Seq(m1, m2))(
      s1,
      MessageContent(m1),
      s2,
      MessageContent(m2)
    )
  }

  // ---------------------------------------------------------------------------
  // ReplayDestination — early termination + skip semantics
  // ---------------------------------------------------------------------------

  it should
    "short-circuit at the matching ReplayDestination, ignoring records that follow it" in {
    val (s1, m1) = (step(1L), msg(1L))
    assertGenerated(Seq(s1), Seq(m1))(
      s1,
      MessageContent(m1),
      ReplayDestination(destA), // <-- replayTo target; iteration stops here
      MessageContent(msg(2L)), // must NOT appear in the result
      step(99L) // must NOT appear either
    )
  }

  it should
    "silently skip a ReplayDestination whose id does not match replayTo (iteration continues)" in {
    val (s1, m1) = (step(1L), msg(1L))
    assertGenerated(Seq(s1), Seq(m1))(
      s1,
      ReplayDestination(destB), // <-- different id; skipped
      MessageContent(m1)
    )
  }

  it should
    "stop at the FIRST matching ReplayDestination when multiple matching records exist" in {
    val s1 = step(1L)
    assertGenerated(Seq(s1), Nil)(
      s1,
      ReplayDestination(destA), // <-- first match
      step(2L), // <-- after the cut
      ReplayDestination(destA)
    )
  }

  // ---------------------------------------------------------------------------
  // Unknown record type — TerminateSignal triggers the `case other` branch
  // ---------------------------------------------------------------------------

  it should "throw RuntimeException on an unhandled record type (e.g. TerminateSignal)" in {
    val ex = intercept[RuntimeException] {
      generateFrom(TerminateSignal)
    }
    assert(
      ex.getMessage.toLowerCase.contains("cannot handle"),
      s"expected diagnostic message about unhandled record, got: ${ex.getMessage}"
    )
  }

  // ---------------------------------------------------------------------------
  // Mixed-record full-cycle
  // ---------------------------------------------------------------------------

  it should "handle a realistic mix of steps + messages + non-matching destinations" in {
    val (s1, s2, s3) = (step(10L), step(20L), step(30L))
    val (m1, m2) = (msg(1L), msg(2L))
    assertGenerated(Seq(s1, s2, s3), Seq(m1, m2))(
      s1,
      MessageContent(m1),
      ReplayDestination(destB), // skipped (id mismatch)
      s2,
      MessageContent(m2),
      ReplayDestination(destB), // also skipped
      s3
    )
  }
}
