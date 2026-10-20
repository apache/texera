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

package org.apache.texera.amber.operator.source.dataset

import org.scalatest.flatspec.AnyFlatSpec

class FileListerSourceOpExecSpec extends AnyFlatSpec {

  "parseDatasetVersionPath" should "extract components from a dataset-prefixed path" in {
    val (prefix, owner, name, version) =
      FileListerSourceOpExec.parseDatasetVersionPath("/dataset/bob@texera.com/twitterDataset/v1")
    assert(prefix == "dataset")
    assert(owner == "bob@texera.com")
    assert(name == "twitterDataset")
    assert(version == "v1")
  }

  it should "work when the owner segment is a username without an '@'" in {
    val (prefix, owner, name, version) =
      FileListerSourceOpExec.parseDatasetVersionPath("/dataset/texera/test-ds/v1")
    assert(owner == "texera")
    assert(name == "test-ds")
    assert(version == "v1")
  }

  it should "ignore trailing slashes and extra segments" in {
    val (prefix, owner, name, version) =
      FileListerSourceOpExec.parseDatasetVersionPath("/dataset/alice/ds/v2/extra/")
    assert(prefix == "dataset")
    assert(owner == "alice")
    assert(name == "ds")
    assert(version == "v2")
  }

  "canonicalVersionPath" should "rebuild the prefixed version path from its components" in {
    assert(
      FileListerSourceOpExec.canonicalVersionPath(
        "dataset",
        "bob@texera.com",
        "twitterDataset",
        "v1"
      ) == "/dataset/bob@texera.com/twitterDataset/v1"
    )
  }

  it should "drop extra segments so emitted file paths stay canonical" in {
    // Emitted paths must be rooted at the parsed components, not the raw configured path:
    // a stray "extra" segment would otherwise leak into every emitted file path.
    val (prefix, owner, name, version) =
      FileListerSourceOpExec.parseDatasetVersionPath("/dataset/alice/ds/v2/extra/")
    assert(
      FileListerSourceOpExec.canonicalVersionPath(prefix, owner, name, version)
        == "/dataset/alice/ds/v2"
    )
  }

  it should "still accept a legacy unprefixed path" in {
    val (prefix, owner, name, version) =
      FileListerSourceOpExec.parseDatasetVersionPath("/alice/ds/v1")
    assert(prefix == "dataset")
    assert(owner == "alice")
    assert(name == "ds")
    assert(version == "v1")
  }

  it should "normalize a legacy unprefixed path to the canonical prefixed form" in {
    val (prefix, owner, name, version) =
      FileListerSourceOpExec.parseDatasetVersionPath("/alice/ds/v1")
    assert(
      FileListerSourceOpExec.canonicalVersionPath(prefix, owner, name, version)
        == "/dataset/alice/ds/v1"
    )
  }

  it should "read an unknown leading segment as a legacy owner" in {
    val (_, owner, name, version) =
      FileListerSourceOpExec.parseDatasetVersionPath("/notAResourceType/alice/ds")
    assert(owner == "notAResourceType")
    assert(name == "alice")
    assert(version == "ds")
  }

  it should "reject a prefixed path with too few segments" in {
    assertThrows[IllegalArgumentException] {
      FileListerSourceOpExec.parseDatasetVersionPath("/dataset/alice/ds")
    }
  }

  it should "reject a legacy path with too few segments" in {
    assertThrows[IllegalArgumentException] {
      FileListerSourceOpExec.parseDatasetVersionPath("/alice/ds")
    }
  }
}
