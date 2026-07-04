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
  * Spec for [[UdfConfig]]. Reading each value forces resolution from udf.conf, so a renamed or
  * mistyped key surfaces here as a ConfigException. Every value carries a `${?ENV}` override, so
  * exact-value assertions are guarded on the override being absent from env and system properties.
  */
class UdfConfigSpec extends AnyFlatSpec with Matchers {

  private def ifUnset(name: String)(assertion: => Any): Unit =
    if (!sys.env.contains(name) && !sys.props.contains(name)) assertion

  "UdfConfig" should "resolve the python and R defaults from udf.conf" in {
    ifUnset("UDF_PYTHON_PATH")(UdfConfig.pythonPath shouldBe "")
    ifUnset("UDF_R_PATH")(UdfConfig.rPath shouldBe "")
    ifUnset("UDF_PYTHON_LOG_STREAMHANDLER_LEVEL")(
      UdfConfig.pythonLogStreamHandlerLevel shouldBe "INFO"
    )
    ifUnset("UDF_PYTHON_LOG_FILEHANDLER_DIR")(UdfConfig.pythonLogFileHandlerDir shouldBe "/tmp/")
    ifUnset("UDF_PYTHON_LOG_FILEHANDLER_LEVEL")(UdfConfig.pythonLogFileHandlerLevel shouldBe "INFO")
  }

  it should "resolve the non-empty log format strings" in {
    ifUnset("UDF_PYTHON_LOG_STREAMHANDLER_FORMAT")(
      UdfConfig.pythonLogStreamHandlerFormat should not be empty
    )
    ifUnset("UDF_PYTHON_LOG_FILEHANDLER_FORMAT")(
      UdfConfig.pythonLogFileHandlerFormat should not be empty
    )
  }
}
