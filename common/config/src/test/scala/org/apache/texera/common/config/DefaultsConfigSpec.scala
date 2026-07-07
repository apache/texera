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
      defaults.get("single_file_upload_max_size_mib") shouldBe Some("20")
    )
    ifUnset("GUI_TABS_HUB_ENABLED")(defaults.get("hub_enabled") shouldBe Some("true"))
    // every value is rendered as a String
    defaults.values.foreach(_ shouldBe a[String])
  }
}
