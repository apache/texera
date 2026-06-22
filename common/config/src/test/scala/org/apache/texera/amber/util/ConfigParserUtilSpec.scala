/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.amber.util

import org.scalatest.flatspec.AnyFlatSpec

class ConfigParserUtilSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Unit-multiplier round-trips
  // ---------------------------------------------------------------------------

  "ConfigParserUtil.parseSizeStringToBytes" should "parse `1KB` to 1024L" in {
    assert(ConfigParserUtil.parseSizeStringToBytes("1KB") == 1024L)
  }

  it should "parse `1MB` to 1024 * 1024 bytes" in {
    assert(ConfigParserUtil.parseSizeStringToBytes("1MB") == 1024L * 1024)
  }

  it should "parse `1GB` to 1024 * 1024 * 1024 bytes" in {
    assert(ConfigParserUtil.parseSizeStringToBytes("1GB") == 1024L * 1024 * 1024)
  }

  // ---------------------------------------------------------------------------
  // Multi-digit values
  // ---------------------------------------------------------------------------

  it should "scale multi-digit values by the unit multiplier (100MB → 104857600 bytes)" in {
    assert(ConfigParserUtil.parseSizeStringToBytes("100MB") == 100L * 1024L * 1024L)
  }

  it should "preserve Long precision for large GB values (5GB)" in {
    // 5 * 1024^3 = 5_368_709_120 — exceeds Int.MaxValue, so Long math is required.
    val expected = 5L * 1024L * 1024L * 1024L
    assert(ConfigParserUtil.parseSizeStringToBytes("5GB") == expected)
    assert(expected > Int.MaxValue.toLong, "the GB result must exceed Int.MaxValue")
  }

  it should "parse multi-digit KB values" in {
    assert(ConfigParserUtil.parseSizeStringToBytes("1024KB") == 1024L * 1024L)
  }

  it should "parse multi-digit GB values" in {
    assert(ConfigParserUtil.parseSizeStringToBytes("128GB") == 128L * 1024L * 1024L * 1024L)
  }

  it should "parse leading-zero values without octal interpretation (`0010KB` == 10KB)" in {
    // The regex captures `\d+`; the value is parsed via `String.toLong`,
    // which treats decimal-only (no octal). Pin this so a future refactor
    // that switched to Integer.decode (which would octal-interpret) is
    // caught.
    assert(ConfigParserUtil.parseSizeStringToBytes("0010KB") == 10L * 1024L)
  }

  // ---------------------------------------------------------------------------
  // Malformed input — IllegalArgumentException
  // ---------------------------------------------------------------------------

  it should "throw IllegalArgumentException when the unit suffix is missing" in {
    val ex = intercept[IllegalArgumentException] {
      ConfigParserUtil.parseSizeStringToBytes("100")
    }
    assert(ex.getMessage.contains("Invalid"))
  }

  it should "throw IllegalArgumentException for an unsupported unit (e.g. TB)" in {
    intercept[IllegalArgumentException] {
      ConfigParserUtil.parseSizeStringToBytes("5TB")
    }
  }

  it should "throw IllegalArgumentException on the empty string" in {
    intercept[IllegalArgumentException] {
      ConfigParserUtil.parseSizeStringToBytes("")
    }
  }

  it should
    "throw IllegalArgumentException for lowercase units (regex is anchored to [KMG]B)" in {
    // Pin case sensitivity — the regex `[KMG]B` requires uppercase
    // letters. `5mb` does NOT match and should fail with the diagnostic.
    intercept[IllegalArgumentException] {
      ConfigParserUtil.parseSizeStringToBytes("5mb")
    }
  }

  it should "throw IllegalArgumentException when the value and unit are separated by whitespace" in {
    // `(\d+)([KMG]B)` is unanchored on the outside but adjacent on the
    // inside; whitespace between value and unit breaks the regex.
    intercept[IllegalArgumentException] {
      ConfigParserUtil.parseSizeStringToBytes("5 MB")
    }
  }

  it should "throw IllegalArgumentException when the value is non-numeric (e.g. `abcMB`)" in {
    intercept[IllegalArgumentException] {
      ConfigParserUtil.parseSizeStringToBytes("abcMB")
    }
  }

  it should "throw IllegalArgumentException when only the unit is supplied (e.g. `MB`)" in {
    intercept[IllegalArgumentException] {
      ConfigParserUtil.parseSizeStringToBytes("MB")
    }
  }

  // ---------------------------------------------------------------------------
  // Return type — Long (not Int)
  // ---------------------------------------------------------------------------

  it should "return a `Long` (compile-time enforced)" in {
    val v: Long = ConfigParserUtil.parseSizeStringToBytes("1MB")
    assert(v == 1024L * 1024L)
  }
}
