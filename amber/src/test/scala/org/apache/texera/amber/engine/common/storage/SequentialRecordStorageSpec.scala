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

package org.apache.texera.amber.engine.common.storage

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.storage.SequentialRecordStorage.{
  SequentialRecordReader,
  SequentialRecordWriter
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.file.{Files, Path}

class SequentialRecordStorageSpec extends AnyFlatSpec with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // Suite-local Pekko serde injected into AmberRuntime via reflection
  // ---------------------------------------------------------------------------
  //
  // `SequentialRecordWriter.writeRecord` / `SequentialRecordReader`'s
  // iterator both hard-code `AmberRuntime.serde`. Pattern matches
  // CheckpointSubsystemSpec / ClientEventSpec: own a suite-local
  // ActorSystem, inject it into AmberRuntime's private vars via
  // reflection, tear down in afterAll.

  private val testSystem: ActorSystem =
    ActorSystem("SequentialRecordStorageSpec-test", AmberRuntime.pekkoConfig)
  private val testSerde: Serialization = SerializationExtension(testSystem)

  private def setAmberRuntimeField(name: String, value: AnyRef): Unit = {
    val field = AmberRuntime.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.set(AmberRuntime, value)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    setAmberRuntimeField("_actorSystem", testSystem)
    setAmberRuntimeField("_serde", testSerde)
  }

  override protected def afterAll(): Unit = {
    setAmberRuntimeField("_serde", null)
    setAmberRuntimeField("_actorSystem", null)
    TestKit.shutdownActorSystem(testSystem)
    super.afterAll()
  }

  // `Files.walk` returns a closeable Stream backed by an open directory
  // handle — wrap in try/finally so the handle is released even if
  // traversal throws, otherwise temp-dir deletion can flake on Windows.
  private def deleteRecursively(p: Path): Unit = {
    if (!Files.exists(p)) return
    val stream = Files.walk(p)
    try {
      stream
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(child => Files.deleteIfExists(child))
    } finally {
      stream.close()
    }
  }

  // ---------------------------------------------------------------------------
  // SequentialRecordWriter + SequentialRecordReader — in-memory round-trip
  // ---------------------------------------------------------------------------
  //
  // Pin the size-prefixed framing contract directly, using
  // ByteArrayInput/OutputStream so the test does not depend on any
  // concrete SequentialRecordStorage subclass (the cross-cutting Reader/
  // Writer pair is what we're characterizing here).

  "SequentialRecordWriter + SequentialRecordReader" should
    "round-trip a sequence of records through a size-prefixed binary frame" in {
    val baos = new ByteArrayOutputStream()
    val writer = new SequentialRecordWriter[String](new DataOutputStream(baos))
    writer.writeRecord("alpha")
    writer.writeRecord("beta")
    writer.writeRecord("gamma")
    writer.flush()
    writer.close()

    val reader = new SequentialRecordReader[String](() =>
      new DataInputStream(new ByteArrayInputStream(baos.toByteArray))
    )
    assert(reader.mkRecordIterator().toList == List("alpha", "beta", "gamma"))
  }

  it should "round-trip an empty stream as a zero-element iterator" in {
    val baos = new ByteArrayOutputStream()
    val writer = new SequentialRecordWriter[String](new DataOutputStream(baos))
    writer.flush()
    writer.close()
    val reader = new SequentialRecordReader[String](() =>
      new DataInputStream(new ByteArrayInputStream(baos.toByteArray))
    )
    assert(reader.mkRecordIterator().toList.isEmpty)
  }

  it should "round-trip a single record" in {
    // The size-prefixed format has the same shape for 1 element as for
    // many, but pinning the 1-record case independently catches an
    // off-by-one in the iterator's prefetch logic.
    val baos = new ByteArrayOutputStream()
    val writer = new SequentialRecordWriter[String](new DataOutputStream(baos))
    writer.writeRecord("only")
    writer.flush()
    writer.close()
    val reader = new SequentialRecordReader[String](() =>
      new DataInputStream(new ByteArrayInputStream(baos.toByteArray))
    )
    val iter = reader.mkRecordIterator()
    assert(iter.hasNext)
    assert(iter.next() == "only")
    assert(!iter.hasNext, "iterator must report exhaustion after the only element is consumed")
  }

  it should "support reading the same byte stream more than once via the inputStreamGen thunk" in {
    // The reader takes a `() => DataInputStream` so it can be re-opened.
    // Two independent calls to mkRecordIterator must each consume their
    // own DataInputStream (constructed by the thunk) and produce the
    // same sequence — proving the thunk is invoked per iterator and that
    // the reader holds no shared mutable input state.
    val baos = new ByteArrayOutputStream()
    val writer = new SequentialRecordWriter[String](new DataOutputStream(baos))
    writer.writeRecord("a")
    writer.writeRecord("b")
    writer.flush()
    writer.close()
    val payload = baos.toByteArray
    val reader = new SequentialRecordReader[String](() =>
      new DataInputStream(new ByteArrayInputStream(payload))
    )
    assert(reader.mkRecordIterator().toList == List("a", "b"))
    assert(reader.mkRecordIterator().toList == List("a", "b"))
  }

  // ---------------------------------------------------------------------------
  // SequentialRecordStorage.fetchAllRecords — companion-level helper
  // ---------------------------------------------------------------------------

  "SequentialRecordStorage.fetchAllRecords" should
    "return every record written to the underlying storage in order" in {
    val tmp = Files.createTempDirectory("seq-storage-fetch-all-")
    val sub = tmp.resolve("logs")
    try {
      val storage = new VFSRecordStorage[String](sub.toUri)
      val writer = storage.getWriter("file-1")
      writer.writeRecord("r1")
      writer.writeRecord("r2")
      writer.writeRecord("r3")
      writer.flush(); writer.close()

      val all = SequentialRecordStorage.fetchAllRecords(storage, "file-1").toList
      assert(all == List("r1", "r2", "r3"))
    } finally {
      deleteRecursively(tmp)
    }
  }

  it should "return an empty Iterable when the underlying reader has no records" in {
    val tmp = Files.createTempDirectory("seq-storage-fetch-empty-")
    val sub = tmp.resolve("logs")
    try {
      val storage = new VFSRecordStorage[String](sub.toUri)
      val writer = storage.getWriter("empty")
      writer.flush(); writer.close()
      assert(SequentialRecordStorage.fetchAllRecords(storage, "empty").toList.isEmpty)
    } finally {
      deleteRecursively(tmp)
    }
  }

  // ---------------------------------------------------------------------------
  // SequentialRecordStorage.getStorage — factory dispatch
  // ---------------------------------------------------------------------------
  //
  // The factory dispatches on the URI scheme. We pin the two
  // schemes that can be exercised without external infrastructure
  // (None → Empty, file:// → VFS). The hdfs:// branch is unit-test-
  // hostile (HDFSRecordStorage's constructor calls FileSystem.get,
  // which can block on DNS / network), so it is deliberately left
  // out of this characterization — the factory's
  // `if (scheme.toLowerCase == "hdfs")` is a single line and any
  // regression there would surface immediately in higher-level
  // checkpoint / fault-tolerance suites that use hdfs:// URIs.

  "SequentialRecordStorage.getStorage" should
    "return an EmptyRecordStorage when the location is None" in {
    val storage = SequentialRecordStorage.getStorage[String](None)
    assert(storage.isInstanceOf[EmptyRecordStorage[_]])
  }

  it should "return a VFSRecordStorage for a file:// URI" in {
    val tmp = Files.createTempDirectory("seq-storage-factory-vfs-")
    val sub = tmp.resolve("logs")
    try {
      val storage = SequentialRecordStorage.getStorage[String](Some(sub.toUri))
      assert(storage.isInstanceOf[VFSRecordStorage[_]])
      // Round-trip a record through the dispatched VFSRecordStorage to
      // prove the factory actually returned a usable instance (not a
      // half-initialized one).
      val w = storage.getWriter("data")
      w.writeRecord("payload")
      w.flush(); w.close()
      val r = storage.getReader("data").mkRecordIterator().toList
      assert(r == List("payload"))
    } finally {
      deleteRecursively(tmp)
    }
  }

  // ---------------------------------------------------------------------------
  // Cross-check — fetchAllRecords composes with the factory-produced storage
  // ---------------------------------------------------------------------------

  "fetchAllRecords on a factory-produced EmptyRecordStorage" should
    "yield zero records" in {
    val storage = SequentialRecordStorage.getStorage[String](None)
    assert(SequentialRecordStorage.fetchAllRecords(storage, "any").toList.isEmpty)
  }
}
