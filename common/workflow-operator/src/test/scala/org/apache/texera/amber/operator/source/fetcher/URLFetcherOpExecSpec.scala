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

package org.apache.texera.amber.operator.source.fetcher

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class URLFetcherOpExecSpec extends AnyFlatSpec with BeforeAndAfter {

  val opDesc: URLFetcherOpDesc = new URLFetcherOpDesc()

  it should "fetch url and output one tuple with raw bytes" in {
    opDesc.url = "https://www.google.com"
    opDesc.decodingMethod = DecodingMethod.RAW_BYTES
    val fetcherOpExec = new URLFetcherOpExec(objectMapper.writeValueAsString(opDesc))
    val iterator = fetcherOpExec.produceTuple()
    assert(iterator.next().getFields.toList.head.isInstanceOf[Array[Byte]])
    assert(!iterator.hasNext)
  }

  it should "fetch url and output one tuple with UTF-8 string" in {
    opDesc.url = "https://www.google.com"
    opDesc.decodingMethod = DecodingMethod.UTF_8
    val fetcherOpExec = new URLFetcherOpExec(objectMapper.writeValueAsString(opDesc))
    val iterator = fetcherOpExec.produceTuple()
    assert(iterator.next().getFields.toList.head.isInstanceOf[String])
    assert(!iterator.hasNext)
  }

  // On a failed fetch the fallback message must interpolate `desc.url` itself,
  // not the descriptor's reflectionToString dump. A file:// URL to a nonexistent
  // path makes getInputStreamFromURL return None deterministically and offline,
  // so the failure branch is exercised without depending on external connectivity.
  it should "report only the URL, not the operator descriptor, when the fetch fails" in {
    opDesc.url = "file:///nonexistent/texera-urlfetcher-regression"
    opDesc.decodingMethod = DecodingMethod.UTF_8
    val fetcherOpExec = new URLFetcherOpExec(objectMapper.writeValueAsString(opDesc))
    val content = fetcherOpExec.produceTuple().next().getFields.toList.head.asInstanceOf[String]

    assert(content == s"Fetch failed for URL: ${opDesc.url}")
    // Guard against the pre-fix `$desc.url` behavior, which leaked the whole
    // descriptor dump (class name + internal fields) into the message.
    assert(!content.contains("URLFetcherOpDesc"))
    assert(!content.contains("operatorId"))
  }

}
