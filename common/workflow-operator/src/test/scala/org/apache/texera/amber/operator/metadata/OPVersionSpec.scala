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

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.errors.GitAPIException
import org.eclipse.jgit.revwalk.RevCommit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.util.Using

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
  * this spec pins the handle to a known state: every test swaps the private static
  * `git` field to a handle it controls (or to null) for its duration and restores
  * the original value afterwards, so the assertions describe one deterministic code
  * path regardless of how the tree was checked out (and no other suite in the JVM
  * is affected).
  *
  * The three resolution outcomes are pinned against throwaway repositories built
  * in a temp directory, so nothing here depends on the checkout the suite runs in:
  *
  *   - a repository with history: the newest commit touching the operator's path,
  *     memoized under the operator name;
  *   - a repository with an unborn HEAD: `LogCommand.call()` raises `NoHeadException`
  *     (a `GitAPIException`) and resolution must not propagate it;
  *   - no handle at all: `git.log()` raises `NullPointerException` and resolution
  *     takes the `"N/A"` fallback.
  *
  * On top of that this spec pins the memoization contract: the answer is cached per
  * operator name and the path is ignored on a cache hit.
  *
  * Deliberately NOT covered: the static initializer itself. It runs once, before any
  * test can observe it, and which of its two branches executes is fixed by how the
  * tree was checked out — no test can flip it without mutating the JVM's environment.
  *
  * Deliberately NOT asserted: the value the `GitAPIException` path returns. That catch
  * leaves `opMap` unpopulated, so the trailing `opMap.get(operatorName)` hands the caller
  * a null version — a latent defect (the `NullPointerException` sibling stores `"N/A"`).
  * Pinning the null would cement it, so only the swallow itself is asserted.
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
    * Runs `body` with `OPVersion`'s private static git handle forced to `handle`, so a
    * chosen resolution path is exercised deterministically, then restores whatever was
    * there.
    */
  private def withGit[T](handle: Git)(body: => T): T = {
    val field = declaredField("git")
    val original = field.get(null)
    field.set(null, handle)
    try body
    finally field.set(null, original)
  }

  private def withNullGit[T](body: => T): T = withGit(null)(body)

  private def withCleanCache[T](names: String*)(body: => T): T =
    try body
    finally names.foreach(opMap.remove)

  private def deleteRecursively(path: Path): Unit =
    if (Files.exists(path)) {
      Using.resource(Files.walk(path)) { stream =>
        stream.iterator().asScala.toSeq.reverse.foreach { p =>
          // best-effort: jgit can keep Windows handles on .git objects; leftover temp
          // files are harmless and get reaped by the OS.
          try Files.deleteIfExists(p)
          catch { case _: java.io.IOException => () }
        }
      }
    }

  /** Runs `body` against a throwaway repository, closing the handle and reaping the directory. */
  private def withTempRepo[T](prefix: String)(body: (Git, Path) => T): T = {
    val dir = Files.createTempDirectory(prefix)
    val handle = Git.init().setDirectory(dir.toFile).call()
    try body(handle, dir)
    finally {
      handle.close()
      deleteRecursively(dir)
    }
  }

  /** Writes `name` and commits just that path; jgit needs an explicit identity here. */
  private def commitFile(handle: Git, dir: Path, name: String, content: String): RevCommit = {
    Files.write(dir.resolve(name), content.getBytes(StandardCharsets.UTF_8))
    handle.add().addFilepattern(name).call()
    handle
      .commit()
      .setMessage(s"touch $name")
      .setAuthor("texera-test", "texera-test@example.com")
      .setCommitter("texera-test", "texera-test@example.com")
      .call()
  }

  // -- resolution against a repository with history ----------------------------

  "OPVersion.getVersion" should "resolve the newest commit that touched the operator's own path" in {
    val alphaOp = uniqueName()
    val betaOp = uniqueName()
    withCleanCache(alphaOp, betaOp) {
      withTempRepo("opversion-history") { (handle, dir) =>
        // Three commits, interleaved so every wrong answer is a different hash from
        // the right one: alpha's answer is neither the tip of the log (that is beta's)
        // nor the oldest commit that touched it.
        val alphaFirst = commitFile(handle, dir, "alpha.txt", "alpha v1")
        val alphaHead = commitFile(handle, dir, "alpha.txt", "alpha v2")
        val betaHead = commitFile(handle, dir, "beta.txt", "beta v1")

        // Guard the fixture: three distinct hashes, so none of the assertions below
        // can pass by coincidence.
        Set(alphaFirst.getName, alphaHead.getName, betaHead.getName) should have size 3

        withGit(handle) {
          OPVersion.getVersion(alphaOp, "alpha.txt") shouldBe alphaHead.getName
          OPVersion.getVersion(betaOp, "beta.txt") shouldBe betaHead.getName
        }
      }
    }
  }

  it should "memoize the resolved commit hash under the operator name" in {
    val name = uniqueName()
    withCleanCache(name) {
      withTempRepo("opversion-memo") { (handle, dir) =>
        val head = commitFile(handle, dir, "alpha.txt", "alpha v1")

        withGit(handle) {
          opMap.containsKey(name) shouldBe false
          OPVersion.getVersion(name, "alpha.txt") shouldBe head.getName
          opMap.get(name) shouldBe head.getName
        }
      }
    }
  }

  it should "swallow a git failure instead of propagating it to the caller" in {
    val name = uniqueName()
    withCleanCache(name) {
      withTempRepo("opversion-unborn") { (handle, _) =>
        // A freshly initialised repository has an unborn HEAD, so LogCommand.call()
        // raises NoHeadException -- a checked GitAPIException, a different catch from
        // the NullPointerException that a missing handle produces. Asserted first so
        // this test cannot pass vacuously if jgit ever starts returning an empty log.
        a[GitAPIException] should be thrownBy
          handle.log().addPath("alpha.txt").setMaxCount(1).call()

        withGit(handle) {
          noException should be thrownBy OPVersion.getVersion(name, "alpha.txt")
        }
      }
    }
  }

  // -- resolution with no usable handle ----------------------------------------

  it should "fall back to \"N/A\" when the git handle is unavailable" in {
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

  // -- memoization contract ----------------------------------------------------

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
