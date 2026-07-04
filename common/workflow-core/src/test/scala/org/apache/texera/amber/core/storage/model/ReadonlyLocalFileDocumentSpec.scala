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

package org.apache.texera.amber.core.storage.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class ReadonlyLocalFileDocumentSpec extends AnyFlatSpec with Matchers {

  private def withTempDoc(
      content: String
  )(body: (ReadonlyLocalFileDocument, File) => Unit): Unit = {
    val tmp = File.createTempFile("readonly-local-", ".txt")
    try {
      Files.write(tmp.toPath, content.getBytes(StandardCharsets.UTF_8))
      body(new ReadonlyLocalFileDocument(tmp.toURI), tmp)
    } finally {
      tmp.delete()
    }
  }

  "ReadonlyLocalFileDocument" should "expose the backing URI and file" in {
    withTempDoc("hello") { (doc, tmp) =>
      doc.getURI shouldBe tmp.toURI
      doc.asFile() shouldBe new File(tmp.toURI)
      doc.asFile().getCanonicalFile shouldBe tmp.getCanonicalFile
    }
  }

  it should "read the file contents through an input stream" in {
    withTempDoc("hello world") { (doc, _) =>
      val in = doc.asInputStream()
      try new String(in.readAllBytes(), "UTF-8") shouldBe "hello world"
      finally in.close()
    }
  }

  it should "not support the collection accessors" in {
    withTempDoc("x") { (doc, _) =>
      intercept[NotImplementedError](doc.getItem(0)).getMessage shouldBe
        "getItem is not supported for ReadonlyLocalFileDocument"
      intercept[NotImplementedError](doc.get()).getMessage shouldBe
        "get is not supported for ReadonlyLocalFileDocument"
      intercept[NotImplementedError](doc.getRange(0, 1)).getMessage shouldBe
        "getRange is not supported for ReadonlyLocalFileDocument"
      intercept[NotImplementedError](doc.getAfter(0)).getMessage shouldBe
        "getAfter is not supported for ReadonlyLocalFileDocument"
      intercept[NotImplementedError](doc.getCount).getMessage shouldBe
        "getCount is not supported for ReadonlyLocalFileDocument"
    }
  }
}
