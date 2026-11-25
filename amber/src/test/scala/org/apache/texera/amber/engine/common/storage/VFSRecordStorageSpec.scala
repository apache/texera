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

import org.apache.texera.amber.engine.common.AmberSerdeTestSupport
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.{Files, Path}

class VFSRecordStorageSpec
    extends AnyFlatSpec
    with AmberSerdeTestSupport
    with RecordStorageTestSupport {

  // ---------------------------------------------------------------------------
  // Temp-directory helpers — every test owns its own scratch folder so the
  // cases are independent and parallel-safe.
  // ---------------------------------------------------------------------------

  // `sub` is the storage folder under a unique temp root and is NOT yet
  // created on disk (so constructor tests can pin the auto-create-folder branch).
  private def withVfsFolder(prefix: String)(body: Path => Unit): Unit =
    withTempFolder(s"vfs-record-storage-spec-$prefix-")(body)

  // ---------------------------------------------------------------------------
  // Constructor — auto-create folder
  // ---------------------------------------------------------------------------

  "VFSRecordStorage constructor" should
    "create the target folder when it does not yet exist" in {
    withVfsFolder("auto-create")(path =>
      assertCreatesFolder(path, new VFSRecordStorage[String](path.toUri))
    )
  }

  it should "leave an existing folder intact" in {
    // If the folder already exists, the constructor's existence check
    // short-circuits and the folder must not be recreated / wiped.
    withVfsFolder("existing")(path =>
      assertPreservesExistingFolder(path, new VFSRecordStorage[String](path.toUri))
    )
  }

  // ---------------------------------------------------------------------------
  // getWriter / getReader — round-trip via the production serde
  // ---------------------------------------------------------------------------

  "VFSRecordStorage.getWriter + getReader" should
    "round-trip a sequence of records through a local file:// URI" in {
    withVfsFolder("round-trip") { path =>
      assertRoundTripsThree(path, new VFSRecordStorage[String](path.toUri))
    }
  }

  it should "produce an empty iterator when reading a file containing no records" in {
    // An empty file (writer opened and closed without writing) must read
    // back as zero records, not throw.
    withVfsFolder("empty-file") { path =>
      val storage = new VFSRecordStorage[String](path.toUri)
      writeRecords(storage, "empty.bin")
      assert(Files.exists(path.resolve("empty.bin")))
      assert(readRecords(storage, "empty.bin").isEmpty)
    }
  }

  it should "support multiple distinct files under the same storage folder" in {
    // Two writers under the same VFSRecordStorage instance must produce
    // independent files — no cross-pollination.
    withVfsFolder("two-files") { path =>
      assertIndependentFiles(new VFSRecordStorage[String](path.toUri))
    }
  }

  // ---------------------------------------------------------------------------
  // deleteStorage
  // ---------------------------------------------------------------------------

  "VFSRecordStorage.deleteStorage" should
    "remove the folder created by the constructor along with its contents" in {
    withVfsFolder("delete") { path =>
      assertDeleteRemovesFolder(path, new VFSRecordStorage[String](path.toUri))
    }
  }

  // ---------------------------------------------------------------------------
  // containsFolder
  // ---------------------------------------------------------------------------

  "VFSRecordStorage.containsFolder" should "return true for an existing sub-folder" in {
    withVfsFolder("contains-folder") { path =>
      val storage = new VFSRecordStorage[String](path.toUri)
      // Create the sub-folder AFTER the storage is constructed so the
      // test pins the live-lookup behavior of containsFolder (not a
      // value cached at construction time).
      Files.createDirectory(path.resolve("nested"))
      assert(storage.containsFolder("nested"))
    }
  }

  it should "return false for a missing child entry" in {
    withVfsFolder("contains-missing") { path =>
      val storage = new VFSRecordStorage[String](path.toUri)
      assert(!storage.containsFolder("does-not-exist"))
    }
  }

  it should "return false when the child entry exists but is a file (not a folder)" in {
    // The `containsFolder` contract is "exists AND isFolder". A plain
    // file with the requested name must NOT register as a folder.
    withVfsFolder("contains-file") { path =>
      val storage = new VFSRecordStorage[String](path.toUri)
      Files.writeString(path.resolve("looks-like-folder"), "i am a file")
      assert(!storage.containsFolder("looks-like-folder"))
    }
  }
}
