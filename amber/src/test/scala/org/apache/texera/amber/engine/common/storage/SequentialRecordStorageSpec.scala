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

import org.apache.commons.io.FileUtils
import org.apache.texera.amber.engine.common.AmberSerdeTestSupport
import org.apache.texera.amber.engine.common.storage.SequentialRecordStorage.{
  SequentialRecordReader,
  SequentialRecordWriter
}
import org.scalatest.flatspec.AnyFlatSpec

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.nio.file.{Files, Path}

private[storage] trait RecordStorageTestSupport {
  this: AnyFlatSpec =>

  protected def withTempFolder(prefix: String)(body: Path => Unit): Unit = {
    val root = Files.createTempDirectory(prefix)
    try body(root.resolve("logs"))
    finally FileUtils.deleteQuietly(root.toFile)
  }

  protected def writeRecords[T >: Null <: AnyRef](
      storage: SequentialRecordStorage[T],
      fileName: String,
      records: T*
  ): Unit = {
    val writer = storage.getWriter(fileName)
    records.foreach(writer.writeRecord)
    writer.flush()
    writer.close()
  }

  protected def readRecords[T >: Null <: AnyRef](
      storage: SequentialRecordStorage[T],
      fileName: String
  ): List[T] =
    storage.getReader(fileName).mkRecordIterator().toList

  protected def assertCreatesFolder[T >: Null <: AnyRef](
      folder: Path,
      open: => SequentialRecordStorage[T]
  ): Unit = {
    assert(!Files.exists(folder), "precondition: folder must not exist before construction")
    val _ = open
    assert(Files.exists(folder), "constructor should auto-create the folder")
    assert(Files.isDirectory(folder))
  }

  protected def assertPreservesExistingFolder[T >: Null <: AnyRef](
      folder: Path,
      open: => SequentialRecordStorage[T]
  ): Unit = {
    Files.createDirectories(folder)
    val sentinel = folder.resolve("sentinel.txt")
    Files.writeString(sentinel, "keep-me")
    val _ = open
    assert(Files.exists(sentinel), "constructor must not delete pre-existing files")
    assert(Files.readString(sentinel) == "keep-me")
  }

  protected def assertRoundTripsThree(
      folder: Path,
      storage: SequentialRecordStorage[String]
  ): Unit = {
    writeRecords(storage, "records.bin", "one", "two", "three")
    assert(Files.exists(folder.resolve("records.bin")))
    assert(readRecords(storage, "records.bin") == List("one", "two", "three"))
  }

  protected def assertIndependentFiles(
      storage: SequentialRecordStorage[String],
      checkFolder: Option[Path] = None
  ): Unit = {
    writeRecords(storage, "a.bin", "from-a")
    writeRecords(storage, "b.bin", "from-b")
    checkFolder.foreach { folder =>
      assert(Files.exists(folder.resolve("a.bin")))
      assert(Files.exists(folder.resolve("b.bin")))
    }
    assert(readRecords(storage, "a.bin") == List("from-a"))
    assert(readRecords(storage, "b.bin") == List("from-b"))
  }

  protected def assertDeleteRemovesFolder(
      folder: Path,
      storage: SequentialRecordStorage[String]
  ): Unit = {
    writeRecords(storage, "data.bin", "payload")
    assert(Files.exists(folder.resolve("data.bin")))
    storage.deleteStorage()
    assert(!Files.exists(folder), "deleteStorage should remove the storage folder")
  }
}

class SequentialRecordStorageSpec
    extends AnyFlatSpec
    with AmberSerdeTestSupport
    with RecordStorageTestSupport {

  private def readerForWrites(records: String*): SequentialRecordReader[String] = {
    val baos = new ByteArrayOutputStream()
    val writer = new SequentialRecordWriter[String](new DataOutputStream(baos))
    records.foreach(writer.writeRecord)
    writer.flush()
    writer.close()
    val payload = baos.toByteArray
    new SequentialRecordReader[String](() => new DataInputStream(new ByteArrayInputStream(payload)))
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
    val reader = readerForWrites("alpha", "beta", "gamma")
    assert(reader.mkRecordIterator().toList == List("alpha", "beta", "gamma"))
  }

  it should "round-trip an empty stream as a zero-element iterator" in {
    val reader = readerForWrites()
    assert(reader.mkRecordIterator().toList.isEmpty)
  }

  it should "round-trip a single record" in {
    // The size-prefixed format has the same shape for 1 element as for
    // many, but pinning the 1-record case independently catches an
    // off-by-one in the iterator's prefetch logic.
    val reader = readerForWrites("only")
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
    val reader = readerForWrites("a", "b")
    assert(reader.mkRecordIterator().toList == List("a", "b"))
    assert(reader.mkRecordIterator().toList == List("a", "b"))
  }

  // ---------------------------------------------------------------------------
  // SequentialRecordStorage.fetchAllRecords — companion-level helper
  // ---------------------------------------------------------------------------

  "SequentialRecordStorage.fetchAllRecords" should
    "return every record written to the underlying storage in order" in {
    withTempFolder("seq-storage-fetch-all-") { sub =>
      val storage = new VFSRecordStorage[String](sub.toUri)
      writeRecords(storage, "file-1", "r1", "r2", "r3")
      val all = SequentialRecordStorage.fetchAllRecords(storage, "file-1").toList
      assert(all == List("r1", "r2", "r3"))
    }
  }

  it should "return an empty Iterable when the underlying reader has no records" in {
    withTempFolder("seq-storage-fetch-empty-") { sub =>
      val storage = new VFSRecordStorage[String](sub.toUri)
      writeRecords(storage, "empty")
      assert(SequentialRecordStorage.fetchAllRecords(storage, "empty").toList.isEmpty)
    }
  }

  // ---------------------------------------------------------------------------
  // SequentialRecordStorage.getStorage — factory dispatch
  // ---------------------------------------------------------------------------
  //
  // The factory dispatches on the URI scheme. We pin the two schemes
  // that need no set-up here (None → Empty, file:// → VFS). The
  // hdfs:// arm is covered by HDFSRecordStorageSpec, which seeds the
  // Hadoop FileSystem cache for the `hdfs` scheme first so that
  // HDFSRecordStorage's internal `FileSystem.get` resolves against a
  // local temp directory instead of reaching for a NameNode.

  "SequentialRecordStorage.getStorage" should
    "return an EmptyRecordStorage when the location is None" in {
    val storage = SequentialRecordStorage.getStorage[String](None)
    assert(storage.isInstanceOf[EmptyRecordStorage[_]])
  }

  it should "return a VFSRecordStorage for a file:// URI" in {
    withTempFolder("seq-storage-factory-vfs-") { sub =>
      val storage = SequentialRecordStorage.getStorage[String](Some(sub.toUri))
      assert(storage.isInstanceOf[VFSRecordStorage[_]])
      // Round-trip a record through the dispatched VFSRecordStorage to
      // prove the factory actually returned a usable instance (not a
      // half-initialized one).
      writeRecords(storage, "data", "payload")
      assert(readRecords(storage, "data") == List("payload"))
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
