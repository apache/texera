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
  * Spec for [[PythonUtils]]. `getPythonExecutable` falls back to "python3" when the configured
  * python path is blank, else returns the trimmed path. The blank-path default depends on
  * UDF_PYTHON_PATH, so the exact-value assertion is guarded on that override being unset.
  */
class PythonUtilsSpec extends AnyFlatSpec with Matchers {

  "PythonUtils.getPythonExecutable" should "fall back to python3 when no python path is configured" in {
    // ${?UDF_PYTHON_PATH} can be satisfied by an OS env var or a JVM system property
    val overrideValue =
      sys.env.get("UDF_PYTHON_PATH").orElse(sys.props.get("UDF_PYTHON_PATH"))
    if (overrideValue.forall(_.trim.isEmpty)) {
      PythonUtils.getPythonExecutable shouldBe "python3"
    }
  }

  it should "never return a blank or untrimmed executable" in {
    val executable = PythonUtils.getPythonExecutable
    executable should not be empty
    executable shouldBe executable.trim
  }

  it should "match its own fallback logic against the backing UdfConfig value" in {
    val expected =
      if (UdfConfig.pythonPath.trim.isEmpty) "python3" else UdfConfig.pythonPath.trim
    PythonUtils.getPythonExecutable shouldBe expected
  }
}
