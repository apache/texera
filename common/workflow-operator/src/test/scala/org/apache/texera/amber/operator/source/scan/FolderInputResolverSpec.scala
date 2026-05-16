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

package org.apache.texera.amber.operator.source.scan

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

class FolderInputResolverSpec extends AnyFlatSpec with Matchers {

  "FolderInputResolver.materializeToLocalPath" should "return a local folder path for folder inputs" in {
    val dir = Files.createTempDirectory("folder-input-resolver-")
    try {
      Files.writeString(dir.resolve("one.txt"), "one")

      FolderInputResolver.materializeToLocalPath(dir.toUri) shouldBe dir
    } finally {
      Files.deleteIfExists(dir.resolve("one.txt"))
      Files.deleteIfExists(dir)
    }
  }
}
