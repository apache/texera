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

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class URLFetcherOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def configured(decoding: DecodingMethod): URLFetcherOpDesc = {
    val op = new URLFetcherOpDesc
    op.url = "https://example.test/data"
    op.decodingMethod = decoding
    op
  }

  "URLFetcherOpDesc.operatorInfo" should "advertise the user-friendly name and API group" in {
    val info = (new URLFetcherOpDesc).operatorInfo
    info.userFriendlyName shouldBe "URL Fetcher"
    info.operatorGroupName shouldBe OperatorGroupConstants.API_GROUP
    info.operatorDescription should include("URL")
  }

  it should "expose no input ports and one output port (source-shaped)" in {
    val info = (new URLFetcherOpDesc).operatorInfo
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "URLFetcherOpDesc.sourceSchema" should "produce a single STRING column when decoding is UTF-8" in {
    val op = configured(DecodingMethod.UTF_8)
    val schema = op.sourceSchema()
    schema.getAttributes should have length 1
    schema.getAttributes.head.getName shouldBe "URL content"
    schema.getAttributes.head.getType shouldBe AttributeType.STRING
  }

  it should "produce a BINARY column for raw-bytes decoding" in {
    val op = configured(DecodingMethod.RAW_BYTES)
    val schema = op.sourceSchema()
    schema.getAttributes should have length 1
    schema.getAttributes.head.getName shouldBe "URL content"
    schema.getAttributes.head.getType shouldBe AttributeType.BINARY
  }

  it should "fail loudly when decodingMethod is left unset rather than silently defaulting to ANY" in {
    // `var decodingMethod: DecodingMethod = _` defaults to null. Without a
    // guard, sourceSchema would fall through `if (decodingMethod ==
    // DecodingMethod.UTF_8) ... else ANY` and silently produce an ANY column
    // for a misconfigured operator. sourceSchema now requires a non-null
    // decodingMethod and surfaces the misconfiguration as an
    // IllegalArgumentException.
    val op = new URLFetcherOpDesc
    op.url = "https://example.test/data"
    an[IllegalArgumentException] should be thrownBy op.sourceSchema()
  }

  "URLFetcherOpDesc.getPhysicalOp" should "wire the URLFetcherOpExec class name into the OpExecInitInfo" in {
    // Pattern-match on OpExecWithClassName instead of substring-matching the
    // toString output, which is brittle to scalapb formatting changes.
    val op = configured(DecodingMethod.UTF_8)
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.source.fetcher.URLFetcherOpExec"
      case other =>
        fail(s"expected OpExecWithClassName, got $other")
    }
  }

  "URLFetcherOpDesc.generateStandaloneCode" should
    "fetch the URL and decode the body as UTF-8 text" in {
    configured(DecodingMethod.UTF_8).generateStandaloneCode() shouldBe
      """import http.client
        |import urllib.request
        |_url = "https://example.test/data"
        |try:
        |    with urllib.request.urlopen(_url) as _resp:
        |        _content = _resp.read()
        |except (OSError, http.client.HTTPException):
        |    _content = f"Fetch failed for URL: {_url}".encode("utf-8")
        |out1df = pd.DataFrame({"URL content": [_content.decode("utf-8")]})""".stripMargin
  }

  // The executor guards only the fetch, so a value with no scheme stops it. `Exception`
  // would swallow that too and hand back a row where the platform stopped.
  it should "let a value that is not a URL through, not just fetch failures" in {
    val code = configured(DecodingMethod.UTF_8).generateStandaloneCode()
    code should include("except (OSError, http.client.HTTPException):")
    code should not include "except Exception:"
  }

  it should "keep the raw bytes when decoding is not UTF-8" in {
    configured(DecodingMethod.RAW_BYTES).generateStandaloneCode() should
      endWith("""out1df = pd.DataFrame({"URL content": [_content]})""")
  }

  // The URL is user-supplied and lands inside the generated Python source, so
  // it goes through JSON string encoding rather than raw interpolation.
  it should "emit the URL as an escaped Python string literal" in {
    val op = configured(DecodingMethod.UTF_8)
    op.url = """https://example.test/a"b\c"""
    op.generateStandaloneCode() should include("""_url = "https://example.test/a\"b\\c"""")
  }

  it should "propagate sourceSchema onto the single output port" in {
    // Exercise propagateSchema.func directly so the test actually proves the
    // sourceSchema gets routed to the output port id, not just that an
    // output port exists. Inputs are empty (this is a source operator).
    val op = configured(DecodingMethod.UTF_8)
    val physical = op.getPhysicalOp(workflowId, executionId)
    val outputPortId = op.operatorInfo.outputPorts.head.id
    val propagated = physical.propagateSchema.func(Map.empty)
    propagated should contain key outputPortId
    propagated(outputPortId) shouldBe op.sourceSchema()
  }
}
