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

import org.scalatest.flatspec.AnyFlatSpec

class EmptyRecordStorageSpec extends AnyFlatSpec {

  // EmptyRecordStorage is the null-object branch of
  // SequentialRecordStorage.getStorage(None). Its reader is backed by a
  // 0-byte NullInputStream and its writer by NullOutputStream — so the
  // reader yields zero records WITHOUT touching AmberRuntime.serde
  // (the iterator's internalNext catches the EOF from readInt() and
  // returns null before deserialize would run).
  //
  // The writer DOES call AmberRuntime.serde.serialize on writeRecord —
  // but every test in this suite avoids writeRecord, exercising only
  // the no-AmberRuntime surface (constructor / flush / close / reader
  // hasNext). This keeps the spec free of the suite-local ActorSystem
  // setup that ClientEventSpec / CheckpointSubsystemSpec need; the
  // serde-touching write path is pinned in SequentialRecordStorageSpec
  // where the harness is set up for it.

  // ---------------------------------------------------------------------------
  // containsFolder
  // ---------------------------------------------------------------------------

  "EmptyRecordStorage.containsFolder" should "return false for any folder name" in {
    val storage = new EmptyRecordStorage[String]()
    assert(!storage.containsFolder("anything"))
    assert(!storage.containsFolder(""))
    assert(!storage.containsFolder("a/b/c"))
  }

  // ---------------------------------------------------------------------------
  // deleteStorage
  // ---------------------------------------------------------------------------

  "EmptyRecordStorage.deleteStorage" should "be a safe no-op" in {
    val storage = new EmptyRecordStorage[String]()
    storage.deleteStorage() // must not throw
    // Idempotent — second call also no-ops.
    storage.deleteStorage()
    succeed
  }

  // ---------------------------------------------------------------------------
  // getReader — zero-record iterator (no serde dependency)
  // ---------------------------------------------------------------------------

  "EmptyRecordStorage.getReader" should
    "return a non-null reader whose iterator is empty (hasNext == false)" in {
    val storage = new EmptyRecordStorage[String]()
    val reader = storage.getReader("any-file")
    assert(reader != null)
    val iter = reader.mkRecordIterator()
    assert(!iter.hasNext, "expected an empty iterator from a NullInputStream-backed reader")
  }

  it should "yield an empty list on toList" in {
    val storage = new EmptyRecordStorage[String]()
    val records = storage.getReader("any-file").mkRecordIterator().toList
    assert(records.isEmpty)
  }

  it should "produce independent empty iterators across successive getReader calls" in {
    // Behavior we want pinned: exhausting one reader does not leak state
    // into a second reader returned by a later getReader call. Independent
    // of whether the two readers happen to be the same instance —
    // mkRecordIterator() on each must produce its own empty iterator.
    val storage = new EmptyRecordStorage[String]()
    val r1 = storage.getReader("a")
    val r2 = storage.getReader("a")
    val _ = r1.mkRecordIterator().toList
    assert(r2.mkRecordIterator().toList.isEmpty)
  }

  it should "ignore the fileName argument (every name produces the same empty iterator)" in {
    // The contract is "any read against an EmptyRecordStorage produces no
    // records" — regardless of fileName.
    val storage = new EmptyRecordStorage[String]()
    assert(storage.getReader("alpha").mkRecordIterator().toList.isEmpty)
    assert(storage.getReader("beta").mkRecordIterator().toList.isEmpty)
    assert(storage.getReader("").mkRecordIterator().toList.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // getWriter — construction & lifecycle without writeRecord
  // ---------------------------------------------------------------------------

  "EmptyRecordStorage.getWriter" should "return a non-null writer" in {
    val storage = new EmptyRecordStorage[String]()
    val writer = storage.getWriter("any-file")
    assert(writer != null)
  }

  it should "allow flush() before any writeRecord without throwing" in {
    val storage = new EmptyRecordStorage[String]()
    val writer = storage.getWriter("any-file")
    writer.flush()
    succeed
  }

  it should "allow close() before any writeRecord without throwing" in {
    val storage = new EmptyRecordStorage[String]()
    val writer = storage.getWriter("any-file")
    writer.close()
    succeed
  }

  it should "keep a second writer usable after the first is closed" in {
    // Behavior we want pinned: closing one writer does not invalidate a
    // second writer returned by a later getWriter call. The two writers
    // must independently support flush/close without interfering.
    val storage = new EmptyRecordStorage[String]()
    val w1 = storage.getWriter("a")
    val w2 = storage.getWriter("a")
    w1.close()
    w2.flush()
    w2.close()
    succeed
  }

  // ---------------------------------------------------------------------------
  // Type parameter erasure — different T must still produce a working
  // storage object. EmptyRecordStorage's behavior is independent of T;
  // pin that with a non-String T to catch any accidental ClassTag misuse.
  // ---------------------------------------------------------------------------

  "EmptyRecordStorage[T]" should "construct cleanly for a different T (java.lang.Integer)" in {
    val storage = new EmptyRecordStorage[java.lang.Integer]()
    assert(!storage.containsFolder("anything"))
    assert(storage.getReader("x").mkRecordIterator().toList.isEmpty)
    storage.getWriter("x").close()
    storage.deleteStorage()
    succeed
  }
}
