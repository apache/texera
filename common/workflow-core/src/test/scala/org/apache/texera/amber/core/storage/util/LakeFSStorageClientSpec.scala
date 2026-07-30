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

package org.apache.texera.amber.core.storage.util

import org.scalatest.flatspec.AnyFlatSpec

class LakeFSStorageClientSpec extends AnyFlatSpec {

  // `healthCheck` retries through the shared `RetryUtil.withBackoff`; that contract (progression,
  // give-up wrapping, interrupt fail-fast) is covered by `RetryUtilSpec` in `common/util`.

  "parsePhysicalAddress" should "split a well-formed address into bucket and key" in {
    assert(
      LakeFSStorageClient.parsePhysicalAddress("s3://my-bucket/path/to/file.csv") ==
        (("my-bucket", "path/to/file.csv"))
    )
    // key should have its leading slash stripped and preserve nested segments
    assert(
      LakeFSStorageClient.parsePhysicalAddress("gs://another-bucket/some/prefix/data.json") ==
        (("another-bucket", "some/prefix/data.json"))
    )
  }

  it should "throw for an empty or blank address" in {
    val emptyEx = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress("")
    }
    assert(emptyEx.getMessage.contains("empty"))

    val blankEx = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress("   ")
    }
    assert(blankEx.getMessage.contains("empty"))
  }

  it should "throw when the address is not a valid URI" in {
    val ex = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress("s3://bad host/key")
    }
    assert(ex.getMessage.contains("Invalid address URI"))
    assert(ex.getCause != null)
  }

  it should "throw when the address is missing a host/bucket" in {
    val ex = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress("s3:///only-a-key")
    }
    assert(ex.getMessage.contains("missing host/bucket"))
  }

  it should "throw when the address is missing a key/path" in {
    val noPathEx = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress("s3://my-bucket")
    }
    assert(noPathEx.getMessage.contains("missing key/path"))

    // a trailing slash yields an empty key after stripping, which is also invalid
    val rootPathEx = intercept[IllegalArgumentException] {
      LakeFSStorageClient.parsePhysicalAddress("s3://my-bucket/")
    }
    assert(rootPathEx.getMessage.contains("missing key/path"))
  }
}
