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

package org.apache.texera.amber.core.storage.model

import org.scalatest.flatspec.AnyFlatSpec

import java.io.{ByteArrayInputStream, File, InputStream}
import java.net.URI

class ReadonlyVirtualDocumentSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Stub impl — provides one sentinel value per accessor so each abstract
  // method can be exercised in isolation, plus an int-typed variant so
  // the type parameter `T` is observed end-to-end.
  // ---------------------------------------------------------------------------

  private class StubReadonlyIntDoc(items: IndexedSeq[Int]) extends ReadonlyVirtualDocument[Int] {
    override def getURI: URI = new URI("file:///stub/int")
    override def getItem(i: Int): Int = items(i)
    override def get(): Iterator[Int] = items.iterator
    override def getRange(from: Int, until: Int, columns: Option[Seq[String]]): Iterator[Int] =
      items.slice(from, until).iterator
    override def getAfter(offset: Int): Iterator[Int] = items.drop(offset + 1).iterator
    override def getCount: Long = items.size.toLong
    override def asInputStream(): InputStream =
      new ByteArrayInputStream(items.map(_.toByte).toArray)
    // Use the OS-portable temp directory rather than a hardcoded
    // `/tmp/...` path so the spec also runs on Windows.
    override def asFile(): File =
      new File(System.getProperty("java.io.tmpdir"), "stub-int")
  }

  // ---------------------------------------------------------------------------
  // Accessor surface — return verbatim
  // ---------------------------------------------------------------------------

  "ReadonlyVirtualDocument.getURI" should "return the impl-supplied URI" in {
    val d = new StubReadonlyIntDoc(IndexedSeq(1, 2, 3))
    assert(d.getURI == new URI("file:///stub/int"))
  }

  "ReadonlyVirtualDocument.getItem" should "delegate to the impl's index lookup" in {
    val d = new StubReadonlyIntDoc(IndexedSeq(10, 20, 30))
    assert(d.getItem(0) == 10)
    assert(d.getItem(2) == 30)
  }

  "ReadonlyVirtualDocument.get" should "iterate every item from the impl in order" in {
    val d = new StubReadonlyIntDoc(IndexedSeq(7, 8, 9))
    assert(d.get().toList == List(7, 8, 9))
  }

  "ReadonlyVirtualDocument.getRange" should
    "yield items in `[from, until)` (half-open interval — `until` is exclusive)" in {
    // Type via the trait so the default-arg contract (`columns: Option[…] = None`)
    // is resolved at the call site through the trait's signature, not the
    // concrete subclass. Scala resolves default parameters from the
    // STATIC type, so a `StubReadonlyIntDoc`-typed value without its own
    // default would not get a default at the call site.
    val d: ReadonlyVirtualDocument[Int] =
      new StubReadonlyIntDoc(IndexedSeq(0, 1, 2, 3, 4))
    assert(d.getRange(1, 4).toList == List(1, 2, 3))
  }

  it should
    "accept the optional `columns` argument and resolve its default (None) when omitted at the call site" in {
    // The `columns` parameter exists so impls can project a subset of
    // columns — `Some(...)` can LEGITIMATELY change the result on
    // column-aware impls (e.g. iceberg-backed documents). What this
    // case pins is the *call-site* contract: the third argument can
    // be supplied or omitted, and the default is `None`.
    //
    // The stub deliberately ignores `columns` so the [from, until)
    // slice is independent of the column selection — that lets us
    // assert the call-site shape without taking a position on how
    // impls should interpret `columns`.
    val d: ReadonlyVirtualDocument[Int] = new StubReadonlyIntDoc(IndexedSeq(0, 1, 2))
    // Call-site with all three positional args.
    assert(d.getRange(0, 2, columns = Some(Seq("c"))).toList == List(0, 1))
    // Call-site that omits the third arg — resolved via the trait's default.
    assert(d.getRange(0, 2).toList == List(0, 1))
  }

  "ReadonlyVirtualDocument.getAfter" should
    "skip the item at index `offset` and yield every item strictly after it" in {
    val d = new StubReadonlyIntDoc(IndexedSeq(0, 1, 2, 3, 4))
    assert(d.getAfter(1).toList == List(2, 3, 4))
  }

  "ReadonlyVirtualDocument.getCount" should "return the item count as a Long" in {
    val d = new StubReadonlyIntDoc(IndexedSeq(0, 1, 2, 3, 4))
    val count: Long = d.getCount
    assert(count == 5L)
  }

  "ReadonlyVirtualDocument.asInputStream" should
    "return a non-null InputStream that reads the impl-supplied bytes" in {
    val d = new StubReadonlyIntDoc(IndexedSeq(7, 8))
    val stream = d.asInputStream()
    try {
      assert(stream.read() == 7)
      assert(stream.read() == 8)
      assert(stream.read() == -1) // EOF
    } finally {
      stream.close()
    }
  }

  "ReadonlyVirtualDocument.asFile" should "return a non-null File from the impl" in {
    val d = new StubReadonlyIntDoc(IndexedSeq.empty)
    val file = d.asFile()
    assert(file != null)
    assert(file.getPath != "")
  }

  // ---------------------------------------------------------------------------
  // Type parameter — T is preserved across accessors
  // ---------------------------------------------------------------------------

  "ReadonlyVirtualDocument[Int]" should
    "preserve the type parameter on every accessor (compile-time enforced)" in {
    // Use a two-item fixture so `getAfter(0)` (a documented, non-negative
    // offset) yields the second item — the trait's docs describe
    // `offset` as a 0-based index, so the test should stay within that
    // documented range.
    val d: ReadonlyVirtualDocument[Int] = new StubReadonlyIntDoc(IndexedSeq(1, 2))
    val item: Int = d.getItem(0)
    val iter: Iterator[Int] = d.get()
    val range: Iterator[Int] = d.getRange(0, 1)
    val after: Iterator[Int] = d.getAfter(0)
    assert(item == 1)
    assert(iter.toList == List(1, 2))
    assert(range.toList == List(1))
    assert(after.toList == List(2))
  }

  // ---------------------------------------------------------------------------
  // Pattern-matching contract
  // ---------------------------------------------------------------------------

  "A ReadonlyVirtualDocument value" should "match the trait via type-pattern" in {
    val d: AnyRef = new StubReadonlyIntDoc(IndexedSeq.empty)
    val matched = d match {
      case _: ReadonlyVirtualDocument[_] => true
      case _                             => false
    }
    assert(matched)
  }
}
