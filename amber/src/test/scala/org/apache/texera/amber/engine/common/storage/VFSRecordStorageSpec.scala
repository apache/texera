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
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI
import java.nio.file.{Files, Path}

class VFSRecordStorageSpec extends AnyFlatSpec with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // Suite-local Pekko serde injected into AmberRuntime via reflection
  // ---------------------------------------------------------------------------
  //
  // `SequentialRecordWriter.writeRecord` hard-codes `AmberRuntime.serde`,
  // so any test that round-trips a record needs AmberRuntime initialized.
  // Pattern matches CheckpointSubsystemSpec / ClientEventSpec — own a
  // suite-local ActorSystem, inject it into AmberRuntime's private vars,
  // tear down in afterAll.

  private val testSystem: ActorSystem =
    ActorSystem("VFSRecordStorageSpec-test", AmberRuntime.pekkoConfig)
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

  // ---------------------------------------------------------------------------
  // Temp-directory helpers — every test owns its own scratch folder so the
  // cases are independent and parallel-safe.
  // ---------------------------------------------------------------------------

  // Returns (sub, uri) where `sub` is the storage folder under a unique
  // temp root and is NOT yet created on disk (so constructor tests can
  // pin the auto-create-folder branch). The parent of `sub` IS the unique
  // temp root, which is what cleanup() removes — keeping the disk clean
  // even when a test fails before the storage folder gets created.
  private def mkTempUri(prefix: String): (Path, URI) = {
    val root = Files.createTempDirectory(s"vfs-record-storage-spec-$prefix-")
    val sub = root.resolve("logs")
    (sub, sub.toUri)
  }

  // Always clean from the parent temp root so any sibling files / partial
  // state created by a failing test are also removed. `Files.walk` returns
  // a closeable Stream backed by an open directory handle — wrap in
  // try/finally so the handle is released even if traversal throws,
  // otherwise temp-dir deletion can flake on Windows.
  private def cleanup(sub: Path): Unit = {
    val root = sub.getParent
    if (root == null || !Files.exists(root)) return
    val stream = Files.walk(root)
    try {
      stream
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(child => Files.deleteIfExists(child))
    } finally {
      stream.close()
    }
  }

  // ---------------------------------------------------------------------------
  // Constructor — auto-create folder
  // ---------------------------------------------------------------------------

  "VFSRecordStorage constructor" should
    "create the target folder when it does not yet exist" in {
    val (path, uri) = mkTempUri("auto-create")
    assert(!Files.exists(path), "precondition: folder must not exist before construction")
    try {
      val _ = new VFSRecordStorage[String](uri)
      assert(Files.exists(path), "constructor should auto-create the folder")
      assert(Files.isDirectory(path))
    } finally {
      cleanup(path)
    }
  }

  it should "leave an existing folder intact" in {
    // If the folder already exists, the constructor's existence check
    // short-circuits and the folder must not be recreated / wiped.
    val (path, uri) = mkTempUri("existing")
    Files.createDirectories(path)
    val sentinel = path.resolve("sentinel.txt")
    Files.writeString(sentinel, "keep-me")
    try {
      val _ = new VFSRecordStorage[String](uri)
      assert(Files.exists(sentinel), "constructor must not delete pre-existing files in the folder")
      assert(Files.readString(sentinel) == "keep-me")
    } finally {
      cleanup(path)
    }
  }

  // ---------------------------------------------------------------------------
  // getWriter / getReader — round-trip via the production serde
  // ---------------------------------------------------------------------------

  "VFSRecordStorage.getWriter + getReader" should
    "round-trip a sequence of records through a local file:// URI" in {
    val (path, uri) = mkTempUri("round-trip")
    try {
      val storage = new VFSRecordStorage[String](uri)
      val writer = storage.getWriter("records.bin")
      writer.writeRecord("one")
      writer.writeRecord("two")
      writer.writeRecord("three")
      writer.flush()
      writer.close()

      // The file produced by getWriter must be visible on disk under the
      // configured folder URI (proves we wrote to the right place).
      assert(Files.exists(path.resolve("records.bin")))

      val records = storage.getReader("records.bin").mkRecordIterator().toList
      assert(records == List("one", "two", "three"))
    } finally {
      cleanup(path)
    }
  }

  it should "produce an empty iterator when reading a file containing no records" in {
    // An empty file (writer opened and closed without writing) must read
    // back as zero records, not throw.
    val (path, uri) = mkTempUri("empty-file")
    try {
      val storage = new VFSRecordStorage[String](uri)
      val writer = storage.getWriter("empty.bin")
      writer.flush()
      writer.close()
      assert(Files.exists(path.resolve("empty.bin")))
      val records = storage.getReader("empty.bin").mkRecordIterator().toList
      assert(records.isEmpty)
    } finally {
      cleanup(path)
    }
  }

  it should "support multiple distinct files under the same storage folder" in {
    // Two writers under the same VFSRecordStorage instance must produce
    // independent files — no cross-pollination.
    val (path, uri) = mkTempUri("two-files")
    try {
      val storage = new VFSRecordStorage[String](uri)
      val w1 = storage.getWriter("a.bin")
      w1.writeRecord("from-a")
      w1.flush(); w1.close()
      val w2 = storage.getWriter("b.bin")
      w2.writeRecord("from-b")
      w2.flush(); w2.close()
      assert(storage.getReader("a.bin").mkRecordIterator().toList == List("from-a"))
      assert(storage.getReader("b.bin").mkRecordIterator().toList == List("from-b"))
    } finally {
      cleanup(path)
    }
  }

  // ---------------------------------------------------------------------------
  // deleteStorage
  // ---------------------------------------------------------------------------

  "VFSRecordStorage.deleteStorage" should
    "remove the folder created by the constructor along with its contents" in {
    val (path, uri) = mkTempUri("delete")
    try {
      val storage = new VFSRecordStorage[String](uri)
      val writer = storage.getWriter("data.bin")
      writer.writeRecord("payload")
      writer.flush(); writer.close()
      assert(Files.exists(path.resolve("data.bin")))

      storage.deleteStorage()
      assert(!Files.exists(path), "deleteStorage should remove the storage folder")
    } finally {
      cleanup(path)
    }
  }

  // ---------------------------------------------------------------------------
  // containsFolder
  // ---------------------------------------------------------------------------

  "VFSRecordStorage.containsFolder" should "return true for an existing sub-folder" in {
    val (path, uri) = mkTempUri("contains-folder")
    try {
      val storage = new VFSRecordStorage[String](uri)
      // Create the sub-folder AFTER the storage is constructed so the
      // test pins the live-lookup behavior of containsFolder (not a
      // value cached at construction time).
      Files.createDirectory(path.resolve("nested"))
      assert(storage.containsFolder("nested"))
    } finally {
      cleanup(path)
    }
  }

  it should "return false for a missing child entry" in {
    val (path, uri) = mkTempUri("contains-missing")
    try {
      val storage = new VFSRecordStorage[String](uri)
      assert(!storage.containsFolder("does-not-exist"))
    } finally {
      cleanup(path)
    }
  }

  it should "return false when the child entry exists but is a file (not a folder)" in {
    // The `containsFolder` contract is "exists AND isFolder". A plain
    // file with the requested name must NOT register as a folder.
    val (path, uri) = mkTempUri("contains-file")
    try {
      val storage = new VFSRecordStorage[String](uri)
      Files.writeString(path.resolve("looks-like-folder"), "i am a file")
      assert(!storage.containsFolder("looks-like-folder"))
    } finally {
      cleanup(path)
    }
  }
}
