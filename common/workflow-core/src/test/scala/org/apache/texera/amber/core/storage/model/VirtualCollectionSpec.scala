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
  // Trait declares four abstract methods — pinned via concrete subclass
  // ---------------------------------------------------------------------------

  "VirtualCollection (concrete subclass)" should "delegate getURI to the implementation" in {
    val c = new StubCollection(uri("file:///tmp/coll"))
    assert(c.getURI == uri("file:///tmp/coll"))
  }

  it should "expose getDocuments as the list of registered child documents (insertion order)" in {
    val c = new StubCollection(uri("file:///coll"))
    assert(c.getDocuments.isEmpty)
    val docA = new StubDocument(uri("file:///coll/a"))
    val docB = new StubDocument(uri("file:///coll/b"))
    c.addChild("a", docA)
    c.addChild("b", docB)
    val docs = c.getDocuments
    assert(docs.size == 2)
    // LinkedHashMap preserves insertion order; pin the URI sequence so
    // a regression to HashMap-backed storage would surface here.
    assert(docs.map(_.getURI) == List(docA.getURI, docB.getURI))
  }

  it should "look up a child by name via getDocument" in {
    val c = new StubCollection(uri("file:///coll"))
    val doc = new StubDocument(uri("file:///coll/only"))
    c.addChild("only", doc)
    // Pin that the same reference is returned (no copy).
    assert(c.getDocument("only") eq doc)
  }

  it should "let getDocument signal a missing child (the spec leaves that to impls)" in {
    // The trait declares `getDocument(name): VirtualDocument[_]` with no
    // exception specification — impls choose how to signal a missing
    // child. The stub raises NoSuchElementException; pin that behavior.
    val c = new StubCollection(uri("file:///coll"))
    intercept[NoSuchElementException] {
      c.getDocument("does-not-exist")
    }
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

  "A VirtualCollection value" should "match the VirtualCollection trait via type-pattern" in {
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
    // refactor that widened the trait to a structural / type-alias
    // declaration.
    val notCol: AnyRef = "hello"
    val matched = notCol match {
      case _: VirtualCollection => true
      case _                    => false
    }
    assert(!matched)
  }
}
