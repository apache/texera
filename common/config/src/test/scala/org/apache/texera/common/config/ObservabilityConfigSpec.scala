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
  * Spec for [[ObservabilityConfig]]. Reading each value forces resolution from
  * observability.conf, so a renamed key surfaces here as a ConfigException.
  * Exact-value assertions are guarded on the OTEL_* override being unset.
  */
class ObservabilityConfigSpec extends AnyFlatSpec with Matchers {

  // `${?VAR}` in HOCON can be satisfied by an OS env var or a JVM system property.
  private def isOverridden(name: String): Boolean =
    sys.env.contains(name) || sys.props.contains(name)

  "ObservabilityConfig" should "default to disabled per issue #5367" in {
    if (!isOverridden(EnvironmentalVariable.ENV_OTEL_SDK_DISABLED)) {
      ObservabilityConfig.sdkDisabled shouldBe "true"
    } else {
      ObservabilityConfig.sdkDisabled should not be empty
    }
  }

  it should "default to a loopback OTLP endpoint" in {
    if (!isOverridden(EnvironmentalVariable.ENV_OTEL_EXPORTER_OTLP_ENDPOINT)) {
      ObservabilityConfig.endpoint shouldBe "http://127.0.0.1:4317"
    } else {
      ObservabilityConfig.endpoint should not be empty
    }
  }

  it should "default the metric export interval to 30s" in {
    if (!isOverridden(EnvironmentalVariable.ENV_OTEL_METRIC_EXPORT_INTERVAL)) {
      ObservabilityConfig.metricExportIntervalMs shouldBe "30000"
    } else {
      ObservabilityConfig.metricExportIntervalMs should not be empty
    }
  }

  it should "resolve resource-attributes and allowed-hosts without error" in {
    // Empty by default; the point is that the keys exist and resolve.
    noException should be thrownBy ObservabilityConfig.resourceAttributes
    noException should be thrownBy ObservabilityConfig.allowedHosts
  }
}
