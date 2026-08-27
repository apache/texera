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

package org.apache.texera.service.resource

import org.apache.texera.common.config.DefaultsConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Unit tests for [[UploadLimits]] and its wiring into [[ResourceStorage]]. Pure: `SiteSettings`
  * falls back to the caller's default when `SqlServer` is uninitialised, so no DB harness is
  * needed. These pin each descriptor's keys against `default.conf` and keep the two types'
  * key sets disjoint — sharing one is how model uploads came to be capped at 20 MiB.
  */
class UploadLimitsSpec extends AnyFlatSpec with Matchers {

  // Each leaf carries a ${?ENV} override, which would legitimately move the default.
  private def ifUnset(name: String)(assertion: => Any): Unit =
    if (!sys.env.contains(name) && !sys.props.contains(name)) assertion

  private def defaultOf(key: String): Option[String] = DefaultsConfig.allDefaults.get(key)

  "UploadLimits.Dataset" should "name keys that default.conf declares, with matching defaults" in {
    UploadLimits.Dataset.all.foreach { limit =>
      withClue(s"${limit.key}: ") {
        defaultOf(limit.key) shouldBe defined
      }
    }
    ifUnset("DATASET_SINGLE_FILE_UPLOAD_MAX_SIZE_MIB")(
      defaultOf("dataset_single_file_upload_max_size_mib") shouldBe Some("20")
    )
    UploadLimits.Dataset.singleFileMaxSizeMiB.defaultValue shouldBe 20L
  }

  "UploadLimits.Model" should "name keys that default.conf declares, with matching defaults" in {
    UploadLimits.Model.all.foreach { limit =>
      withClue(s"${limit.key}: ") {
        defaultOf(limit.key) shouldBe defined
      }
    }
    ifUnset("MODEL_SINGLE_FILE_UPLOAD_MAX_SIZE_MIB")(
      defaultOf("model_single_file_upload_max_size_mib") shouldBe Some("2048")
    )
    UploadLimits.Model.singleFileMaxSizeMiB.defaultValue shouldBe 2048L
  }

  it should "not share a single key with the dataset limits" in {
    val datasetKeys = UploadLimits.Dataset.all.map(_.key).toSet
    val modelKeys = UploadLimits.Model.all.map(_.key).toSet
    modelKeys should have size 4
    datasetKeys intersect modelKeys shouldBe empty
  }

  "singleFileUploadMaxBytes" should "convert the declared MiB default to bytes when no row exists" in {
    ifUnset("DATASET_SINGLE_FILE_UPLOAD_MAX_SIZE_MIB")(
      UploadLimits.Dataset.singleFileUploadMaxBytes shouldBe 20L * 1024L * 1024L
    )
    // The regression: models used to resolve the dataset key and land on 20 MiB.
    ifUnset("MODEL_SINGLE_FILE_UPLOAD_MAX_SIZE_MIB") {
      UploadLimits.Model.singleFileUploadMaxBytes shouldBe 2048L * 1024L * 1024L
      UploadLimits.Model.singleFileUploadMaxBytes should be >
        UploadLimits.Dataset.singleFileUploadMaxBytes
    }
  }

  "ResourceStorage" should "hand each resource type its own limits" in {
    ResourceStorage.Dataset.uploadLimits shouldBe UploadLimits.Dataset
    ResourceStorage.Model.uploadLimits shouldBe UploadLimits.Model
  }
}
