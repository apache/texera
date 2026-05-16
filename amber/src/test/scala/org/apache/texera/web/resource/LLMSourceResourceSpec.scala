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

package org.apache.texera.web.resource

import org.apache.texera.amber.config.PythonUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class LLMSourceResourceSpec extends AnyFlatSpec with Matchers {

  "LLMSourceResource.pythonBinaryCandidates" should "prefer the configured worker interpreter" in {
    new LLMSourceResource().pythonBinaryCandidates.head shouldBe PythonUtils.getPythonExecutable
  }

  "LLMSourceResource.resolveRuntimeInputPath" should "keep a folder input as a folder for dry-runs" in {
    val dir = Files.createTempDirectory("llm-source-folder-dry-run-")
    try {
      Files.writeString(dir.resolve("a.txt"), "a")
      Files.writeString(dir.resolve("b.txt"), "b")

      val resolved = new LLMSourceResource().resolveRuntimeInputPath(dir.toUri)

      Files.isDirectory(resolved) shouldBe true
      resolved shouldBe dir
    } finally {
      Files.deleteIfExists(dir.resolve("a.txt"))
      Files.deleteIfExists(dir.resolve("b.txt"))
      Files.deleteIfExists(dir)
    }
  }
}
