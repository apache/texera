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

package org.apache.texera.amber.engine.architecture.pythonworker

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.charset.StandardCharsets
import java.util.Base64

class PythonWorkflowWorkerStartupConfigSpec extends AnyFlatSpec {

  private def decode(encoded: String): String =
    new String(Base64.getDecoder.decode(encoded), StandardCharsets.UTF_8)

  "encodeStartupConfig" should "serialize entries to a Base64-encoded JSON object keyed by name" in {
    val encoded = PythonWorkflowWorker.encodeStartupConfig(
      Seq("workerId" -> "w-1", "outputPort" -> "5005", "s3Region" -> "us-west-2")
    )
    val parsed = objectMapper.readValue(decode(encoded), classOf[java.util.Map[String, String]])
    assert(parsed.get("workerId") == "w-1")
    assert(parsed.get("outputPort") == "5005")
    assert(parsed.get("s3Region") == "us-west-2")
    assert(parsed.size() == 3)
  }

  it should "produce output free of quotes and whitespace so it survives argv quoting on Windows" in {
    val encoded = PythonWorkflowWorker.encodeStartupConfig(
      Seq("workerId" -> "w-1", "s3Region" -> "us-west-2")
    )
    assert(!encoded.exists(c => c == '"' || c.isWhitespace))
  }

  it should "fail loudly when the same key appears more than once" in {
    val exception = intercept[IllegalArgumentException] {
      PythonWorkflowWorker.encodeStartupConfig(
        Seq("s3Region" -> "us-west-2", "s3Region" -> "us-east-1")
      )
    }
    assert(exception.getMessage.contains("duplicate"))
  }

  private val expectedKeys = Set(
    "workerId",
    "outputPort",
    "loggerLevel",
    "rPath",
    "icebergCatalogType",
    "icebergPostgresCatalogUriWithoutScheme",
    "icebergPostgresCatalogUsername",
    "icebergPostgresCatalogPassword",
    "icebergRestCatalogUri",
    "icebergRestCatalogWarehouseName",
    "icebergTableNamespace",
    "icebergTableStateNamespace",
    "icebergFileStorageDirectoryPath",
    "icebergTableCommitBatchSize",
    "s3Endpoint",
    "s3Region",
    "s3AuthUsername",
    "s3AuthPassword",
    "s3LargeBinariesBaseUri"
  )

  "buildStartupConfig" should "produce exactly the expected named keys with the worker values" in {
    val config =
      PythonWorkflowWorker.buildStartupConfig("worker-7", "6000", "/opt/R", "s3://bucket/uri")
    val map = config.toMap

    assert(config.size == expectedKeys.size, "no duplicate or missing keys")
    assert(map.keySet == expectedKeys)
    assert(map("workerId") == "worker-7")
    assert(map("outputPort") == "6000")
    assert(map("rPath") == "/opt/R")
    assert(map("s3LargeBinariesBaseUri") == "s3://bucket/uri")
  }

  it should "produce a config that round-trips through encodeStartupConfig" in {
    val encoded = PythonWorkflowWorker.encodeStartupConfig(
      PythonWorkflowWorker.buildStartupConfig("w", "1", "", "uri")
    )
    val parsed = objectMapper.readValue(decode(encoded), classOf[java.util.Map[String, String]])
    assert(parsed.get("workerId") == "w")
    assert(parsed.get("s3LargeBinariesBaseUri") == "uri")
    assert(parsed.size() == expectedKeys.size)
  }
}
