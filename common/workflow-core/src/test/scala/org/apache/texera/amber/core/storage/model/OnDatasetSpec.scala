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

class OnDatasetSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Test-only concrete subclass — exposes the three accessors the trait
  // declares. `OnDataset` is purely a contract; the behavior to pin is
  // that a value of type `OnDataset` can be constructed, that its accessors
  // return the values passed in, and that pattern-matching against the
  // trait works for cross-module dispatch.
  // ---------------------------------------------------------------------------

  private class StubOnDataset(repo: String, hash: String, relPath: String) extends OnDataset {
    override def getRepositoryName(): String = repo
    override def getVersionHash(): String = hash
    override def getFileRelativePath(): String = relPath
  }

  /**
    * A second, distinct concrete impl — used to verify the trait is open
    * (not sealed) and that *different* implementations (not just different
    * field values) coexist correctly. Returns fixed strings to make the
    * test independent of any constructor arguments.
    */
  private class HardcodedOnDataset extends OnDataset {
    override def getRepositoryName(): String = "fixed-repo"
    override def getVersionHash(): String = "fixed-hash"
    override def getFileRelativePath(): String = "fixed/path"
  }

  // ---------------------------------------------------------------------------
  // Accessor surface — return verbatim
  // ---------------------------------------------------------------------------

  "OnDataset.getRepositoryName" should "return the value supplied by the concrete impl" in {
    val ds: OnDataset = new StubOnDataset("my-repo", "abc123", "data/file.csv")
    assert(ds.getRepositoryName() == "my-repo")
  }

  "OnDataset.getVersionHash" should "return the value supplied by the concrete impl" in {
    val ds: OnDataset = new StubOnDataset("my-repo", "abc123", "data/file.csv")
    assert(ds.getVersionHash() == "abc123")
  }

  "OnDataset.getFileRelativePath" should "return the value supplied by the concrete impl" in {
    val ds: OnDataset = new StubOnDataset("my-repo", "abc123", "data/file.csv")
    assert(ds.getFileRelativePath() == "data/file.csv")
  }

  // ---------------------------------------------------------------------------
  // Empty / unusual values are passed through unchanged
  // ---------------------------------------------------------------------------

  "OnDataset accessors" should
    "pass empty strings through unchanged (the trait imposes no validation)" in {
    val ds: OnDataset = new StubOnDataset("", "", "")
    assert(ds.getRepositoryName() == "")
    assert(ds.getVersionHash() == "")
    assert(ds.getFileRelativePath() == "")
  }

  it should
    "return distinct values across two genuinely distinct concrete implementations" in {
    // Use a different concrete class (HardcodedOnDataset) — not just a
    // second StubOnDataset instance — so the test actually exercises the
    // trait's open-extension contract and confirms different impls
    // implement getRepositoryName / getVersionHash / getFileRelativePath
    // independently.
    val a: OnDataset = new StubOnDataset("repo-a", "h-a", "p-a")
    val b: OnDataset = new HardcodedOnDataset
    assert(a.getRepositoryName() != b.getRepositoryName())
    assert(a.getVersionHash() != b.getVersionHash())
    assert(a.getFileRelativePath() != b.getFileRelativePath())
    assert(b.getRepositoryName() == "fixed-repo")
    assert(b.getVersionHash() == "fixed-hash")
    assert(b.getFileRelativePath() == "fixed/path")
  }

  // ---------------------------------------------------------------------------
  // Pattern-matching contract — consumers branch on `case _: OnDataset`
  // ---------------------------------------------------------------------------

  "An OnDataset value" should "match the OnDataset trait via type-pattern" in {
    val ds: OnDataset = new StubOnDataset("r", "h", "p")
    val matched: Boolean = ds match {
      case _: OnDataset => true
      case _            => false
    }
    assert(matched)
  }

  it should
    "not match a different unrelated type via type-pattern (sanity check)" in {
    // Asymmetry sanity: a String is NOT an OnDataset. This catches a
    // refactor that accidentally widened `OnDataset` to a structural /
    // type-alias declaration (e.g. `type OnDataset = AnyRef`), which
    // would make every reference type match the pattern.
    val notDs: AnyRef = "hello"
    val matched: Boolean = notDs match {
      case _: OnDataset => true
      case _            => false
    }
    assert(!matched)
  }
}
