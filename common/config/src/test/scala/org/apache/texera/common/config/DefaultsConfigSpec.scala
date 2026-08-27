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
  * Spec for [[DefaultsConfig]]. Reading each value forces resolution from default.conf, so a
  * renamed or mistyped key surfaces here as a ConfigException. The `reinit` flag carries a
  * `${?ENV}` override, so its exact-value assertion is guarded.
  */
class DefaultsConfigSpec extends AnyFlatSpec with Matchers {

  private def ifUnset(name: String)(assertion: => Any): Unit =
    if (!sys.env.contains(name) && !sys.props.contains(name)) assertion

  "DefaultsConfig.reinit" should "default the reset-to-defaults flag to false" in {
    ifUnset("CONFIG_SERVICE_ALWAYS_RESET_CONFIGURATIONS_TO_DEFAULT_VALUES")(
      DefaultsConfig.reinit shouldBe false
    )
  }

  "DefaultsConfig.allDefaults" should "flatten default.conf into short-key/value entries" in {
    val defaults = DefaultsConfig.allDefaults
    defaults should not be empty
    // scalar leaves are flattened to their last path segment
    ifUnset("DATASET_SINGLE_FILE_UPLOAD_MAX_SIZE_MIB")(
      defaults.get("dataset_single_file_upload_max_size_mib") shouldBe Some("20")
    )
    ifUnset("GUI_TABS_HUB_ENABLED")(defaults.get("hub_enabled") shouldBe Some("true"))
    // the model block's leaves are `model_`-prefixed to keep short keys unique
    ifUnset("MODEL_SINGLE_FILE_UPLOAD_MAX_SIZE_MIB")(
      defaults.get("model_single_file_upload_max_size_mib") shouldBe Some("2048")
    )
    ifUnset("GUI_TABS_MODELS_ENABLED")(defaults.get("models_enabled") shouldBe Some("false"))
    // management-only keys are flattened too (used by reset + the startup seeder)
    ifUnset("OPERATOR_CSV_PARSER_MAX_COLUMNS")(
      defaults.get("csv_parser_max_columns") shouldBe Some("512")
    )
    // every value is rendered as a String
    defaults.values.foreach(_ shouldBe a[String])
  }

  it should "keep management-only keys out of the public gui/dataset/model whitelist" in {
    // csv_parser_max_columns is seeded and resettable (present in allDefaults)
    // but lives under `operator`, so it must never reach the anonymous
    // /config/settings/public payload.
    DefaultsConfig.allDefaults.keySet should contain("csv_parser_max_columns")
    DefaultsConfig.keysUnderSections(
      Set("gui", "dataset", "model")
    ) should not contain "csv_parser_max_columns"
  }

  "DefaultsConfig.keysUnderSections" should "collect the short keys of the requested sections only" in {
    val guiKeys = DefaultsConfig.keysUnderSections(Set("gui"))
    guiKeys should contain allOf ("logo", "mini_logo", "favicon", "hub_enabled")
    // keys from other sections are excluded
    guiKeys should not contain "dataset_single_file_upload_max_size_mib"
    guiKeys should not contain "always-reset-configurations-to-default-values"

    val datasetKeys = DefaultsConfig.keysUnderSections(Set("dataset"))
    datasetKeys should contain("dataset_single_file_upload_max_size_mib")
    datasetKeys should not contain "logo"

    // `model` is a sibling section, not a sub-section of `dataset`: disjoint sets
    // are what let a model carry a different ceiling.
    val modelKeys = DefaultsConfig.keysUnderSections(Set("model"))
    modelKeys should contain("model_single_file_upload_max_size_mib")
    modelKeys should not contain "dataset_single_file_upload_max_size_mib"
    datasetKeys should not contain "model_single_file_upload_max_size_mib"
  }

  it should "union multiple sections and be empty for an unknown section" in {
    val union = DefaultsConfig.keysUnderSections(Set("gui", "dataset", "model"))
    union should contain allOf ("logo", "dataset_single_file_upload_max_size_mib", "model_single_file_upload_max_size_mib")
    // every returned key exists in allDefaults under the same short name
    union.subsetOf(DefaultsConfig.allDefaults.keySet) shouldBe true

    DefaultsConfig.keysUnderSections(Set("no-such-section")) shouldBe empty
    DefaultsConfig.keysUnderSections(Set.empty) shouldBe empty
  }
}
