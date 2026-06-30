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

package org.apache.texera.common.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Spec for the staged-file-cleanup settings added to [[StorageConfig]] (PR #5643).
  *
  * Reading each value forces [[StorageConfig]] to resolve it from storage.conf, so a renamed
  * or mistyped key surfaces here as a ConfigException instead of at service start-up.
  */
class StorageConfigSpec extends AnyFlatSpec with Matchers {

  "StorageConfig cleanup settings" should "load positive retention and interval windows" in {
    StorageConfig.cleanupRetentionHours should be > 0
    StorageConfig.cleanupIntervalMinutes should be > 0
  }

  it should "default the cleanup job to disabled so merging never auto-enables deletion" in {
    // The job deletes uncommitted dataset data, so it must be opt-in per deployment via
    // STORAGE_CLEANUP_ENABLED; this guards the safe default from silently flipping to true.
    // Only assert when the env override is unset (e.g. in CI), since it would win otherwise.
    if (sys.env.get(StorageConfig.ENV_CLEANUP_ENABLED).isEmpty) {
      StorageConfig.cleanupEnabled shouldBe false
    }
  }

  it should "expose the expected environment-variable override names" in {
    StorageConfig.ENV_CLEANUP_ENABLED shouldBe "STORAGE_CLEANUP_ENABLED"
    StorageConfig.ENV_CLEANUP_RETENTION_HOURS shouldBe "STORAGE_CLEANUP_RETENTION_HOURS"
    StorageConfig.ENV_CLEANUP_INTERVAL_MINUTES shouldBe "STORAGE_CLEANUP_INTERVAL_MINUTES"
  }
}
