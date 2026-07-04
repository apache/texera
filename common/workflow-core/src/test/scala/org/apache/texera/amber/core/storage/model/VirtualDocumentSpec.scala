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

import java.io.ByteArrayInputStream
import java.net.URI

class VirtualDocumentSpec extends AnyFlatSpec with Matchers {

  // A minimal concrete document overriding only the two abstract members
  // (getURI, clear); every other method keeps its default throwing body.
  private class MinimalDoc extends VirtualDocument[Int] {
    override def getURI: URI = new URI("file:///stub/doc")
    override def clear(): Unit = ()
  }

  // one shared instance: every default method throws statelessly, so all assertions can run
  // against the same document (in particular the URI and clear checks below)
  private val doc: VirtualDocument[Int] = new MinimalDoc

  "VirtualDocument" should "return the implemented URI and support clear" in {
    doc.getURI shouldBe new URI("file:///stub/doc")
    noException should be thrownBy doc.clear()
  }

  it should "throw NotImplementedError for every unimplemented read accessor" in {
    intercept[NotImplementedError](doc.getItem(0)).getMessage shouldBe
      "getItem method is not implemented"
    intercept[NotImplementedError](doc.get()).getMessage shouldBe "get method is not implemented"
    intercept[NotImplementedError](doc.getRange(0, 1)).getMessage shouldBe
      "getRange method is not implemented"
    intercept[NotImplementedError](doc.getRange(0, 1, columns = Some(Seq("c")))).getMessage shouldBe
      "getRange method is not implemented"
    intercept[NotImplementedError](doc.getAfter(0)).getMessage shouldBe
      "getAfter method is not implemented"
    intercept[NotImplementedError](doc.getCount).getMessage shouldBe
      "getCount method is not implemented"
  }

  it should "throw NotImplementedError for every unimplemented write accessor" in {
    intercept[NotImplementedError](doc.setItem(0, 5)).getMessage shouldBe
      "setItem method is not implemented"
    // note: the writer default reports the message "write method is not implemented"
    intercept[NotImplementedError](doc.writer("w")).getMessage shouldBe
      "write method is not implemented"
    intercept[NotImplementedError](doc.append(5)).getMessage shouldBe
      "append method is not implemented"
    intercept[NotImplementedError](doc.append(Iterator(1, 2))).getMessage shouldBe
      "append method is not implemented"
    intercept[NotImplementedError](
      doc.appendStream(new ByteArrayInputStream(Array[Byte](1)))
    ).getMessage shouldBe "appendStream method is not implemented"
  }

  it should "throw NotImplementedError for the unimplemented file/stat accessors" in {
    intercept[NotImplementedError](doc.asInputStream()).getMessage shouldBe
      "asInputStream method is not implemented"
    intercept[NotImplementedError](doc.asFile()).getMessage shouldBe
      "asFile method is not implemented"
    intercept[NotImplementedError](doc.getTableStatistics).getMessage shouldBe
      "getTableStatistics method is not implemented"
    intercept[NotImplementedError](doc.getTotalFileSize).getMessage shouldBe
      "getTotalFileSize method is not implemented"
  }
}
