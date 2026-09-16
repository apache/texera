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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.serialization.{Serialization, SerializationExtension}
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.util.WinutilsFreeLocalFileSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI
import java.nio.file.{Files, Path}
import java.util.UUID

/**
  * Characterizes [[HDFSRecordStorage]] — the `hdfs://` arm of
  * `SequentialRecordStorage.getStorage`, used by the fault-tolerance / checkpoint
  * subsystem when the log location points at a Hadoop cluster.
  *
  * What this spec catches:
  *   - the scheme guard silently accepting a non-`hdfs` URI (which would make
  *     HDFSRecordStorage and VFSRecordStorage fight over the same location),
  *   - the constructor no longer auto-creating the log folder (writers then fail
  *     on the very first checkpoint), or wiping a folder that already has logs,
  *   - a writer/reader framing or path-resolution regression, i.e. records not
  *     coming back in write order or landing outside the configured folder,
  *   - `getReader` throwing instead of degrading to `EmptyRecordStorage`'s
  *     zero-record reader when a log file was never written (recovery of a
  *     worker that never checkpointed depends on that fallback),
  *   - `containsFolder` degrading into a plain `exists` check (a same-named
  *     *file* must not answer "yes, that folder is here"),
  *   - `deleteStorage` throwing when the folder is already gone.
  *
  * No HDFS cluster and no native Hadoop install is needed — see
  * `withSeededHdfs` below for how the `hdfs` scheme is served locally.
  */
class HDFSRecordStorageSpec extends AnyFlatSpec with BeforeAndAfterAll {

  // ---------------------------------------------------------------------------
  // Suite-local Pekko serde injected into AmberRuntime via reflection
  // ---------------------------------------------------------------------------
  //
  // `SequentialRecordWriter.writeRecord` / `SequentialRecordReader`'s iterator
  // both hard-code `AmberRuntime.serde`, so any test that round-trips a record
  // needs AmberRuntime initialized. Pattern matches VFSRecordStorageSpec /
  // SequentialRecordStorageSpec — own a suite-local ActorSystem, inject it into
  // AmberRuntime's private vars, tear down in afterAll.

  // `lazy` matters: BeforeAndAfterAll skips beforeAll/afterAll entirely when the
  // active filter selects no test from this suite (which is what the
  // amber-integration CI job does via AMBER_TEST_FILTER). A strict `val` would
  // still build an ActorSystem during suite construction and never shut it down.
  private lazy val testSystem: ActorSystem =
    ActorSystem("HDFSRecordStorageSpec-test", AmberRuntime.pekkoConfig)
  private lazy val testSerde: Serialization = SerializationExtension(testSystem)

  private def getAmberRuntimeField(name: String): AnyRef = {
    val field = AmberRuntime.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.get(AmberRuntime)
  }

  private def setAmberRuntimeField(name: String, value: AnyRef): Unit = {
    val field = AmberRuntime.getClass.getDeclaredField(name)
    field.setAccessible(true)
    field.set(AmberRuntime, value)
  }

  private var previousActorSystem: AnyRef = _
  private var previousSerde: AnyRef = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    previousActorSystem = getAmberRuntimeField("_actorSystem")
    previousSerde = getAmberRuntimeField("_serde")
    setAmberRuntimeField("_actorSystem", testSystem)
    setAmberRuntimeField("_serde", testSerde)
  }

  override protected def afterAll(): Unit = {
    // Restore whatever was there rather than nulling: sbt runs amber's suites
    // concurrently in one JVM and several of them install their own serde into
    // these same globals. Nulling makes `AmberRuntime.serde` take its
    // lazy-bootstrap branch mid-run in a sibling suite, which spins up an
    // untracked cluster ActorSystem; restoring leaves the globals as we found them.
    setAmberRuntimeField("_serde", previousSerde)
    setAmberRuntimeField("_actorSystem", previousActorSystem)
    TestKit.shutdownActorSystem(testSystem)
    super.afterAll()
  }

  // ---------------------------------------------------------------------------
  // Serving the `hdfs` scheme locally, with no cluster and no winutils
  // ---------------------------------------------------------------------------
  //
  // HDFSRecordStorage builds its own `new Configuration()` internally, so a test
  // cannot inject one. What a test CAN do is get to the FileSystem cache first:
  // `FileSystem.CACHE` is keyed on (scheme, authority, UGI) ONLY — the
  // Configuration's *contents* are not part of the key. So we call
  // `FileSystem.get(hdfs://<authority>/, conf)` ourselves, with our own
  // `fs.hdfs.impl`, BEFORE constructing the class under test. That is the
  // ordinary supported `FileSystem.get` path (no reflection anywhere): it
  // populates the cache legitimately, and HDFSRecordStorage's own
  // `FileSystem.get(uri, freshConf)` for the same (scheme, authority) is then
  // served from that entry — `fs.hdfs.impl` is never resolved again and no
  // NameNode is ever contacted.
  //
  // The implementation registered under `hdfs` is WinutilsFreeLocalFileSystem
  // (common/workflow-core main scope, on amber's compile classpath via
  // WorkflowCompiler -> WorkflowOperator -> WorkflowCore). Plain
  // `LocalFileSystem` cannot be used here: on Windows its first `mkdirs` goes
  // through `RawLocalFileSystem.setPermission` -> `Shell.getWinUtilsPath` and
  // fails with "Hadoop bin directory does not exist" on a host with no native
  // Hadoop. The shim skips permission ops *only* on winutils-less Windows
  // hosts and otherwise delegates to Hadoop's default local file system, so
  // this spec runs unchanged on Linux/macOS CI.

  private def withSeededHdfs(prefix: String)(body: (URI, Path) => Unit): Unit = {
    val root = Files.createTempDirectory(s"hdfs-record-storage-spec-$prefix-")
    // A unique authority per test means a unique cache key per test: tests can
    // neither see each other's FileSystem instance nor its working directory.
    val authority = s"hdfs-record-storage-spec-${UUID.randomUUID()}:9000"
    val conf = new Configuration()
    conf.set("fs.hdfs.impl", classOf[WinutilsFreeLocalFileSystem].getName)
    val fs = FileSystem.get(URI.create(s"hdfs://$authority/"), conf)
    try {
      // HDFSRecordStorage resolves its folder as
      // `Path.mergePaths(fs.getWorkingDirectory, new Path(uri.getPath))`, so
      // pointing the working directory at our temp root nests the URI's
      // `/logs` path underneath it.
      fs.setWorkingDirectory(new org.apache.hadoop.fs.Path(root.toUri.toString))
      body(URI.create(s"hdfs://$authority/logs"), root.resolve("logs"))
    } finally {
      // `FileSystem.close()` removes only this instance's own key, so the
      // JVM-global cache is left exactly as we found it. Close before walking
      // the temp tree: an open handle makes deletion flake on Windows.
      fs.close()
      cleanup(root)
    }
  }

  // Always clean from the unique temp root so partial state left by a failing
  // test is removed too. `Files.walk` returns a closeable Stream backed by an
  // open directory handle — close it in a finally, otherwise temp-dir deletion
  // can flake on Windows.
  private def cleanup(root: Path): Unit = {
    if (!Files.exists(root)) return
    val stream = Files.walk(root)
    try {
      stream
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(child => Files.deleteIfExists(child))
    } catch {
      // Best-effort: this runs from a `finally`, so an un-deletable leftover
      // (a handle a *failing* test never closed) must not replace the real
      // assertion failure with an AccessDeniedException from cleanup.
      case _: java.io.IOException => ()
    } finally {
      stream.close()
    }
  }

  // ---------------------------------------------------------------------------
  // Constructor — scheme guard
  // ---------------------------------------------------------------------------

  "HDFSRecordStorage constructor" should "reject a non-hdfs URI" in {
    // A file:// location belongs to VFSRecordStorage. The guard must also fire
    // *before* any file-system work, so nothing is created on disk — that is
    // what the second assertion pins.
    val root = Files.createTempDirectory("hdfs-record-storage-spec-bad-scheme-")
    val folder = root.resolve("logs")
    try {
      intercept[AssertionError] {
        new HDFSRecordStorage[String](folder.toUri)
      }
      assert(!Files.exists(folder), "a rejected URI must not create anything on disk")
    } finally {
      cleanup(root)
    }
  }

  // ---------------------------------------------------------------------------
  // Constructor — auto-create folder
  // ---------------------------------------------------------------------------

  it should "create the log folder when it does not yet exist" in {
    withSeededHdfs("auto-create") { (uri, folder) =>
      assert(!Files.exists(folder), "precondition: folder must not exist before construction")
      val _ = new HDFSRecordStorage[String](uri)
      assert(Files.exists(folder), "constructor should auto-create the folder")
      assert(Files.isDirectory(folder))
    }
  }

  it should "leave an existing folder intact" in {
    // If the folder already exists the constructor's existence check
    // short-circuits; pre-existing logs must survive re-opening the storage.
    withSeededHdfs("existing") { (uri, folder) =>
      Files.createDirectories(folder)
      val sentinel = folder.resolve("sentinel.txt")
      Files.writeString(sentinel, "keep-me")
      val _ = new HDFSRecordStorage[String](uri)
      assert(Files.exists(sentinel), "constructor must not delete pre-existing files")
      assert(Files.readString(sentinel) == "keep-me")
    }
  }

  // ---------------------------------------------------------------------------
  // getWriter / getReader — round-trip via the production serde
  // ---------------------------------------------------------------------------

  "HDFSRecordStorage.getWriter + getReader" should
    "round-trip a sequence of records in write order" in {
    withSeededHdfs("round-trip") { (uri, folder) =>
      val storage = new HDFSRecordStorage[String](uri)
      val writer = storage.getWriter("records.bin")
      writer.writeRecord("one")
      writer.writeRecord("two")
      writer.writeRecord("three")
      writer.flush()
      writer.close()

      // The file must materialize *inside* the resolved log folder, i.e. under
      // `mergePaths(fs.getWorkingDirectory, uri.getPath)`, which the fixture
      // pins to root/logs. Note this is deliberately NOT a claim that the file
      // lands at the URI's authority-rooted path: production appends the URI
      // path to the working directory, so against a real NameNode
      // `hdfs://nn:9000/logs` resolves under the user's home, not at `/logs`.
      assert(Files.exists(folder.resolve("records.bin")))

      val records = storage.getReader("records.bin").mkRecordIterator().toList
      assert(records == List("one", "two", "three"))
    }
  }

  it should "keep distinct files under the same storage folder independent" in {
    // Two writers from one HDFSRecordStorage must not cross-pollinate; the
    // payloads differ so a swapped-path bug cannot pass.
    withSeededHdfs("two-files") { (uri, folder) =>
      val storage = new HDFSRecordStorage[String](uri)
      val w1 = storage.getWriter("a.bin")
      w1.writeRecord("from-a")
      w1.flush(); w1.close()
      val w2 = storage.getWriter("b.bin")
      w2.writeRecord("from-b")
      w2.flush(); w2.close()
      assert(Files.exists(folder.resolve("a.bin")))
      assert(Files.exists(folder.resolve("b.bin")))
      assert(storage.getReader("a.bin").mkRecordIterator().toList == List("from-a"))
      assert(storage.getReader("b.bin").mkRecordIterator().toList == List("from-b"))
    }
  }

  it should "overwrite (not append to) an existing file when the writer is re-opened" in {
    // `getWriter` goes through `FileSystem.create(path)`, whose default is
    // overwrite=true. A re-opened writer must therefore replace the old
    // contents — appending would resurrect stale records during recovery.
    withSeededHdfs("overwrite") { (uri, _) =>
      val storage = new HDFSRecordStorage[String](uri)
      val first = storage.getWriter("log.bin")
      first.writeRecord("stale-1")
      first.writeRecord("stale-2")
      first.flush(); first.close()

      val second = storage.getWriter("log.bin")
      second.writeRecord("fresh")
      second.flush(); second.close()

      assert(storage.getReader("log.bin").mkRecordIterator().toList == List("fresh"))
    }
  }

  // ---------------------------------------------------------------------------
  // getReader — EmptyRecordStorage fallback for an absent file
  // ---------------------------------------------------------------------------

  "HDFSRecordStorage.getReader" should
    "fall back to an empty reader when the requested file does not exist" in {
    withSeededHdfs("absent-file") { (uri, folder) =>
      val storage = new HDFSRecordStorage[String](uri)
      // Populate a *different* file first: the fallback must key off the
      // requested file's existence, not the folder being empty, and must not
      // hand back the sibling's records.
      val writer = storage.getWriter("present.bin")
      writer.writeRecord("sibling-payload")
      writer.flush(); writer.close()
      assert(!Files.exists(folder.resolve("missing.bin")), "precondition: file must be absent")

      val iter = storage.getReader("missing.bin").mkRecordIterator()
      assert(!iter.hasNext, "absent file must yield a zero-element iterator")
      assert(storage.getReader("missing.bin").mkRecordIterator().toList.isEmpty)
      // Sanity: the present file is still readable, so the fallback did not
      // break the normal path.
      assert(storage.getReader("present.bin").mkRecordIterator().toList == List("sibling-payload"))
    }
  }

  // ---------------------------------------------------------------------------
  // deleteStorage
  // ---------------------------------------------------------------------------

  "HDFSRecordStorage.deleteStorage" should
    "remove the log folder along with its contents" in {
    withSeededHdfs("delete") { (uri, folder) =>
      val storage = new HDFSRecordStorage[String](uri)
      val writer = storage.getWriter("data.bin")
      writer.writeRecord("payload")
      writer.flush(); writer.close()
      assert(Files.exists(folder.resolve("data.bin")))

      storage.deleteStorage()
      assert(!Files.exists(folder), "deleteStorage should remove the storage folder recursively")
    }
  }

  it should "be a safe no-op when the folder is already gone" in {
    // Second call takes the `!exists` branch. Recovery paths call
    // deleteStorage defensively, so it must be idempotent rather than throw.
    withSeededHdfs("delete-twice") { (uri, folder) =>
      val storage = new HDFSRecordStorage[String](uri)
      storage.deleteStorage()
      assert(!Files.exists(folder))
      storage.deleteStorage()
      assert(!Files.exists(folder))
    }
  }

  // ---------------------------------------------------------------------------
  // containsFolder
  // ---------------------------------------------------------------------------

  "HDFSRecordStorage.containsFolder" should "return true for an existing sub-folder" in {
    withSeededHdfs("contains-folder") { (uri, folder) =>
      val storage = new HDFSRecordStorage[String](uri)
      // Create the sub-folder AFTER construction so the test pins the
      // live-lookup behavior of containsFolder (not a construction-time cache).
      Files.createDirectory(folder.resolve("nested"))
      assert(storage.containsFolder("nested"))
    }
  }

  it should "return false for a missing child entry" in {
    withSeededHdfs("contains-missing") { (uri, _) =>
      val storage = new HDFSRecordStorage[String](uri)
      assert(!storage.containsFolder("does-not-exist"))
    }
  }

  it should "return false when the child entry exists but is a file (not a folder)" in {
    // The contract is "exists AND isDirectory". An `exists`-only
    // implementation would wrongly answer true here.
    withSeededHdfs("contains-file") { (uri, folder) =>
      val storage = new HDFSRecordStorage[String](uri)
      Files.writeString(folder.resolve("looks-like-folder"), "i am a file")
      assert(!storage.containsFolder("looks-like-folder"))
    }
  }

  // ---------------------------------------------------------------------------
  // SequentialRecordStorage.getStorage — the hdfs:// dispatch arm
  // ---------------------------------------------------------------------------
  //
  // This is how production actually reaches HDFSRecordStorage, so pin the
  // dispatch here (SequentialRecordStorageSpec covers the None / file:// arms).

  // Subject is scoped to the hdfs arm so the two files do not report overlapping
  // test names: SequentialRecordStorageSpec owns the bare
  // "SequentialRecordStorage.getStorage" subject for the None / file:// arms.
  "SequentialRecordStorage.getStorage (hdfs arm)" should "route an hdfs:// URI to HDFSRecordStorage" in {
    withSeededHdfs("factory") { (uri, folder) =>
      val storage = SequentialRecordStorage.getStorage[String](Some(uri))
      assert(storage.isInstanceOf[HDFSRecordStorage[_]])
      // Round-trip through the dispatched instance so the assertion proves a
      // usable storage, not just a type tag.
      val writer = storage.getWriter("data.bin")
      writer.writeRecord("via-factory")
      writer.flush(); writer.close()
      assert(Files.exists(folder.resolve("data.bin")))
      assert(storage.getReader("data.bin").mkRecordIterator().toList == List("via-factory"))
    }
  }

  it should "route an upper-case HDFS:// URI to HDFSRecordStorage" in {
    // Both the factory's dispatch and the constructor's guard compare
    // `scheme.toLowerCase`; dropping either would break an upper-case URI
    // (the factory would silently pick VFSRecordStorage, or the constructor
    // would throw). The FileSystem cache key is scheme/authority-lowercased,
    // so the instance seeded under `hdfs` still serves this URI.
    withSeededHdfs("factory-upper-case") { (uri, folder) =>
      val upperCase = URI.create(s"HDFS://${uri.getAuthority}${uri.getPath}")
      val storage = SequentialRecordStorage.getStorage[String](Some(upperCase))
      assert(storage.isInstanceOf[HDFSRecordStorage[_]])
      val writer = storage.getWriter("upper.bin")
      writer.writeRecord("upper-case-scheme")
      writer.flush(); writer.close()
      assert(Files.exists(folder.resolve("upper.bin")))
      assert(storage.getReader("upper.bin").mkRecordIterator().toList == List("upper-case-scheme"))
    }
  }
}
