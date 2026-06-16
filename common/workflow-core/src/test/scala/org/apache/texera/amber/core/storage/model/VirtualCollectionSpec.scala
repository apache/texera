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

import java.net.URI
import scala.collection.mutable

class VirtualCollectionSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Test harness — a minimal in-memory concrete impl exercises every
  // abstract method (getURI / getDocuments / getDocument / remove).
  //
  // The contained `VirtualDocument`s are stubbed with the smallest
  // concrete impl: `clear()` and `getURI` are the only abstract members
  // not given a default by the base class.
  // ---------------------------------------------------------------------------

  private class StubDocument(uriValue: URI) extends VirtualDocument[Nothing] {
    override def getURI: URI = uriValue
    override def clear(): Unit = ()
  }

  private class StubCollection(uriValue: URI) extends VirtualCollection {
    private val children = mutable.LinkedHashMap.empty[String, VirtualDocument[_]]
    private var removed = false

    def addChild(name: String, doc: VirtualDocument[_]): Unit = children(name) = doc
    def wasRemoved: Boolean = removed

    override def getURI: URI = uriValue
    override def getDocuments: List[VirtualDocument[_]] = children.values.toList
    override def getDocument(name: String): VirtualDocument[_] =
      children.getOrElse(name, throw new NoSuchElementException(name))
    override def remove(): Unit = {
      children.clear()
      removed = true
    }
  }

  private def uri(s: String): URI = new URI(s)

  // ---------------------------------------------------------------------------
  // Abstract class declares four abstract methods — pinned via a
  // concrete subclass. (VirtualCollection is an `abstract class`, not a
  // trait — see `VirtualCollection.scala`.)
  // ---------------------------------------------------------------------------

  "VirtualCollection (concrete subclass)" should "delegate getURI to the implementation" in {
    val c = new StubCollection(uri("file:///tmp/coll"))
    assert(c.getURI == uri("file:///tmp/coll"))
  }

  it should "expose getDocuments as a list whose membership matches every addChild call" in {
    // The `VirtualCollection` API does NOT document an ordering
    // guarantee on `getDocuments`, so assert on membership (the set
    // of URIs) rather than exact sequence — over-constraining future
    // impls is more brittle than under-constraining.
    val c = new StubCollection(uri("file:///coll"))
    assert(c.getDocuments.isEmpty)
    val docA = new StubDocument(uri("file:///coll/a"))
    val docB = new StubDocument(uri("file:///coll/b"))
    c.addChild("a", docA)
    c.addChild("b", docB)
    val docs = c.getDocuments
    assert(docs.size == 2)
    assert(docs.map(_.getURI).toSet == Set(docA.getURI, docB.getURI))
  }

  it should "look up a child by name via getDocument" in {
    val c = new StubCollection(uri("file:///coll"))
    val doc = new StubDocument(uri("file:///coll/only"))
    c.addChild("only", doc)
    // Pin that the same reference is returned (no copy).
    assert(c.getDocument("only") eq doc)
  }

  it should "let an impl decide how to signal a missing child (not pinned by the abstract class)" in {
    // The abstract class declares `getDocument(name): VirtualDocument[_]`
    // with no exception specification — impls choose how to signal a
    // missing child. Avoid pinning a specific exception type here so
    // the case does not become an implicit contract on every future
    // impl; just pin that the call does NOT silently return a
    // (legitimate) document for an unregistered name.
    val c = new StubCollection(uri("file:///coll"))
    val outcome = scala.util.Try(c.getDocument("does-not-exist"))
    assert(
      outcome.isFailure,
      s"a missing-child lookup must signal failure (it MUST NOT silently return a document); got: $outcome"
    )
  }

  // ---------------------------------------------------------------------------
  // remove — irreversible side effect
  // ---------------------------------------------------------------------------

  "VirtualCollection.remove" should
    "clear the collection of children (impl-defined side effect)" in {
    val c = new StubCollection(uri("file:///coll"))
    c.addChild("d", new StubDocument(uri("file:///coll/d")))
    assert(c.getDocuments.size == 1)
    c.remove()
    assert(c.getDocuments.isEmpty)
    assert(c.wasRemoved)
  }

  // ---------------------------------------------------------------------------
  // Type-pattern matching — `case _: VirtualCollection`
  // ---------------------------------------------------------------------------

  "A VirtualCollection value" should "match the VirtualCollection type via type-pattern" in {
    val c: AnyRef = new StubCollection(uri("file:///coll"))
    val matched = c match {
      case _: VirtualCollection => true
      case _                    => false
    }
    assert(matched)
  }

  it should
    "NOT match an unrelated type via type-pattern (sanity check)" in {
    // Asymmetry sanity: a String is not a VirtualCollection. Catches a
    // refactor that widened the abstract class to a structural / type-alias
    // declaration.
    val notCol: AnyRef = "hello"
    val matched = notCol match {
      case _: VirtualCollection => true
      case _                    => false
    }
    assert(!matched)
  }
}
