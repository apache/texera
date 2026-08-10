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

package org.apache.texera.amber.operator.metadata

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

/**
  * `OPVersion` resolves an operator's version from the git history of the file
  * that defines it, memoizing the answer in a process-wide static map.
  *
  * Its git handle is opened once in a static initializer against `TEXERA_HOME`
  * (defaulting to the working directory). Whether that succeeds depends entirely
  * on how the tree was checked out — inside a `git worktree` the `.git` entry is
  * a file rather than a directory and jgit raises `RepositoryNotFoundException`,
  * leaving the handle null; in a plain clone the handle opens and an unknown path
  * yields an empty log instead. Rather than assert whatever both happen to share,
  * this spec pins the handle to a known state: every test forces the private
  * static `git` field to null for its duration and restores the original value
  * afterwards, so the assertions describe one deterministic code path regardless
  * of how the tree was checked out (and no other suite in the JVM is affected).
  *
  * With the handle null, `git.log()` throws `NullPointerException` and resolution
  * takes the `"N/A"` fallback. On top of that this spec pins the memoization
  * contract: the answer is cached per operator name and the path is ignored on a
  * cache hit.
  *
  * Deliberately NOT covered: the success path that returns a real commit hash and
  * the `GitAPIException` catch (which, note, leaves `opMap` unpopulated and so
  * returns null). Both require a specific, openable repository state that is not
  * guaranteed for a test run.
  */
class OPVersionSpec extends AnyFlatSpec with Matchers {

  /** The cache is static and shared, so every test uses a name nothing else can collide with. */
  private def uniqueName(): String = s"OPVersionSpec-${UUID.randomUUID()}"

  private def uniqueMissingPath(): String = s"no/such/operator/path/${UUID.randomUUID()}"

  private def declaredField(name: String): java.lang.reflect.Field = {
    val field = classOf[OPVersion].getDeclaredField(name)
    field.setAccessible(true)
    field
  }

  /** The private static memo table, so tests can seed it and clean up after themselves. */
  private def opMap: java.util.Map[String, String] = {
    declaredField("opMap").get(null).asInstanceOf[java.util.Map[String, String]]
  }

  /**
    * Runs `body` with `OPVersion`'s private static git handle forced to null, so the
    * fallback path is exercised deterministically, then restores whatever was there.
    */
  private def withNullGit[T](body: => T): T = {
    val field = declaredField("git")
    val original = field.get(null)
    field.set(null, null)
    try body
    finally field.set(null, original)
  }

  private def withCleanCache[T](names: String*)(body: => T): T =
    try body
    finally names.foreach(opMap.remove)

  "OPVersion.getVersion" should "fall back to \"N/A\" when the git handle is unavailable" in {
    val name = uniqueName()
    withCleanCache(name) {
      withNullGit {
        OPVersion.getVersion(name, uniqueMissingPath()) shouldBe "N/A"
      }
    }
  }

  it should "never return null, whatever it resolves" in {
    val name = uniqueName()
    withCleanCache(name) {
      withNullGit {
        OPVersion.getVersion(name, "common/workflow-operator/src/main/scala") should not be null
      }
    }
  }

  it should "memoize the resolved version under the operator name" in {
    val name = uniqueName()
    withCleanCache(name) {
      withNullGit {
        opMap.containsKey(name) shouldBe false
        val resolved = OPVersion.getVersion(name, uniqueMissingPath())
        opMap.containsKey(name) shouldBe true
        opMap.get(name) shouldBe resolved
      }
    }
  }

  it should "serve the memoized value and ignore the path on subsequent calls" in {
    val name = uniqueName()
    withCleanCache(name) {
      withNullGit {
        // Seed a value no lookup could ever produce: if the cache were bypassed,
        // the call below would recompute and answer "N/A" instead.
        opMap.put(name, "seeded-version")
        OPVersion.getVersion(name, uniqueMissingPath()) shouldBe "seeded-version"
        OPVersion.getVersion(name, "a/completely/different/path") shouldBe "seeded-version"
      }
    }
  }

  it should "key the cache by operator name rather than by path" in {
    val first = uniqueName()
    val second = uniqueName()
    withCleanCache(first, second) {
      withNullGit {
        opMap.put(first, "version-of-first")
        opMap.put(second, "version-of-second")

        val sharedPath = "common/workflow-operator/src/main/scala"
        OPVersion.getVersion(first, sharedPath) shouldBe "version-of-first"
        OPVersion.getVersion(second, sharedPath) shouldBe "version-of-second"
      }
    }
  }

  it should "resolve unknown paths consistently across repeated calls" in {
    val name = uniqueName()
    withCleanCache(name) {
      withNullGit {
        val path = uniqueMissingPath()
        val first = OPVersion.getVersion(name, path)
        val second = OPVersion.getVersion(name, path)
        first shouldBe "N/A"
        second shouldBe first
      }
    }
  }
}
