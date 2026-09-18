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

package org.apache.texera.amber.operator.keywordSearch

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorMetadataGenerator}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.queryparser.classic.QueryParser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.regex.Pattern
import scala.util.Try

class KeywordSearchOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def newDesc(attr: String, kw: String, caseSensitive: Boolean): KeywordSearchOpDesc = {
    val d = new KeywordSearchOpDesc
    d.attribute = attr
    d.keyword = kw
    d.isCaseSensitive = caseSensitive
    d
  }

  "KeywordSearchOpDesc.operatorInfo" should
    "advertise the name, Search group, and reconfiguration support" in {
    val info = (new KeywordSearchOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Keyword Search"
    info.operatorGroupName shouldBe OperatorGroupConstants.SEARCH_GROUP
    info.operatorDescription.toLowerCase should include("keyword")
    info.supportReconfiguration shouldBe true
  }

  it should "expose exactly one input port and one output port" in {
    val info = (new KeywordSearchOpDesc).operatorInfo
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "KeywordSearchOpDesc.isCaseSensitive" should "default to false" in {
    (new KeywordSearchOpDesc).isCaseSensitive shouldBe false
  }

  "KeywordSearchOpDesc.getPhysicalOp" should "wire the KeywordSearchOpExec class name" in {
    val physical =
      newDesc("col", "needle", caseSensitive = false).getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.keywordSearch.KeywordSearchOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
  }

  it should "carry forward the operatorInfo input/output port identities" in {
    val op = newDesc("col", "needle", caseSensitive = false)
    val physical = op.getPhysicalOp(workflowId, executionId)
    // Pin the actual port identities (not just counts), so a drift in port
    // wiring is caught even when the number of ports is unchanged.
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  /** The `pattern` the keyword field injects into its schema, read from the schema
    * itself rather than restated here, so the test cannot pass against a stale copy.
    */
  private val keywordPattern: String = {
    val property = OperatorMetadataGenerator
      .generateOperatorJsonSchema(classOf[KeywordSearchOpDesc])
      .path("properties")
      .path("keyword")
    property.path("pattern").asText()
  }

  /** Whether the parser the executor builds accepts the value at all. */
  private def parses(keyword: String): Boolean =
    Try(new QueryParser("col", new StandardAnalyzer()).parse(keyword)).isSuccess

  // A value the field should take, and whether the pattern is meant to admit it. Quoting
  // is the only query syntax in play in each, so the verdict belongs to QueryParser, and
  // every case is put to it below rather than decided here.
  private val keywords: Seq[(String, Boolean)] = Seq(
    "hello" -> true,
    "\"a b\"" -> true, // a phrase query: the quotes are paired
    "a \"b\" c" -> true,
    "\"a\" \"b\"" -> true,
    "he\\\"llo" -> true, // an escaped quote is a character in a term, not a delimiter
    "\\\"unclosed" -> true,
    "\"a \\\" b\"" -> true, // an escaped quote inside a phrase leaves the pair intact
    "a\\\\\"b\"" -> true, // an escaped backslash, then a phrase
    "he\"llo" -> false,
    "\"unclosed" -> false,
    "x\"y\"z\"" -> false,
    "\\\\\"" -> false, // the backslash is escaped, so the quote is the bare one
    "end\\" -> false // a backslash with nothing left to escape
  )

  behavior of "The keyword pattern"

  it should "be present in the generated schema" in {
    keywordPattern should not be empty
  }

  keywords.foreach {
    case (value, isValid) =>
      val verb = if (isValid) "accept" else "reject"
      it should s"$verb '$value', as QueryParser does" in {
        // find() rather than matches(), because the form validates with
        // `new RegExp().test`, which searches instead of anchoring. An unanchored
        // pattern would pass every value here.
        Pattern.compile(keywordPattern).matcher(value).find() shouldBe isValid
        parses(value) shouldBe isValid
      }
  }

  "KeywordSearchOpDesc JSON round-trip" should
    "preserve attribute, keyword, and isCaseSensitive via the polymorphic base" in {
    val json = objectMapper.writeValueAsString(newDesc("title", "apache", caseSensitive = true))
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[KeywordSearchOpDesc]
    val kw = restored.asInstanceOf[KeywordSearchOpDesc]
    kw.attribute shouldBe "title"
    kw.keyword shouldBe "apache"
    kw.isCaseSensitive shouldBe true
  }
}
