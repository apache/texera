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

package org.apache.texera.amber.operator.unneststring

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorMetadataGenerator}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UnnestStringOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def newDesc(delim: String, attr: String, result: String): UnnestStringOpDesc = {
    val d = new UnnestStringOpDesc
    d.delimiter = delim
    d.attribute = attr
    d.resultAttribute = result
    d
  }

  "UnnestStringOpDesc.operatorInfo" should "advertise the name and Utility group" in {
    val info = (new UnnestStringOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Unnest String"
    info.operatorGroupName shouldBe OperatorGroupConstants.UTILITY_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "UnnestStringOpDesc.getPhysicalOp" should "wire UnnestStringOpExec and carry port identities" in {
    val op = newDesc(",", "tags", "tag")
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.unneststring.UnnestStringOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "UnnestStringOpDesc schema propagation" should
    "append a STRING column named by resultAttribute" in {
    val op = newDesc(",", "tags", "tag")
    val physical = op.getPhysicalOp(workflowId, executionId)
    val input = Schema().add(new Attribute("tags", AttributeType.STRING))
    val out = physical.propagateSchema.func(Map(op.operatorInfo.inputPorts.head.id -> input))
    out shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> input.add(new Attribute("tag", AttributeType.STRING))
    )
  }

  it should "throw when resultAttribute is blank" in {
    val op = newDesc(",", "tags", "")
    val physical = op.getPhysicalOp(workflowId, executionId)
    val input = Schema().add(new Attribute("tags", AttributeType.STRING))
    intercept[RuntimeException] {
      physical.propagateSchema.func(Map(op.operatorInfo.inputPorts.head.id -> input))
    }
  }

  it should "report a delimiter that is not a valid regular expression" in {
    val op = newDesc("(", "tags", "tag")
    val physical = op.getPhysicalOp(workflowId, executionId)
    val input = Schema().add(new Attribute("tags", AttributeType.STRING))
    val ex = intercept[RuntimeException] {
      physical.propagateSchema.func(Map(op.operatorInfo.inputPorts.head.id -> input))
    }
    ex.getMessage should startWith("Delimiter is not a valid regular expression")
  }

  it should "name the problem for each common regex mistake" in {
    val input = Schema().add(new Attribute("tags", AttributeType.STRING))
    // unclosed group, unclosed class, nothing to repeat, reversed range, trailing escape
    List("(", "[", "*", "a{2,1}", "\\").foreach { bad =>
      withClue(s"pattern '$bad': ") {
        val op = newDesc(bad, "tags", "tag")
        val physical = op.getPhysicalOp(workflowId, executionId)
        val ex = intercept[RuntimeException] {
          physical.propagateSchema.func(Map(op.operatorInfo.inputPorts.head.id -> input))
        }
        ex.getMessage should startWith("Delimiter is not a valid regular expression")
        ex.getMessage should include("near index")
      }
    }
  }

  private def compileError(delimiter: String): Option[String] = {
    val op = newDesc(delimiter, "tags", "tag")
    val input = Schema().add(new Attribute("tags", AttributeType.STRING))
    try {
      op.getPhysicalOp(workflowId, executionId)
        .propagateSchema
        .func(Map(op.operatorInfo.inputPorts.head.id -> input))
      None
    } catch { case e: RuntimeException => Some(e.getMessage) }
  }

  // Checked with Java's engine, so patterns the browser can't parse (inline flags) or reads
  // differently (\p{..}) are judged by what the operator actually runs.
  it should "reject a pattern that matches the empty string, which splits every character apart" in {
    List("|", "\\s*", "(?i)", "(?s).*", "\\z", "\\Q\\E").foreach { pattern =>
      withClue(s"pattern '$pattern': ") {
        compileError(pattern).getOrElse("") should include("matches an empty string")
      }
    }
  }

  it should "reject a pattern that matches every character, line breaks included" in {
    List("(?s).", "[\\s\\S]").foreach { pattern =>
      withClue(s"pattern '$pattern': ") {
        compileError(pattern).getOrElse("") should include(
          "matches every character, so nothing would be left"
        )
      }
    }
  }

  // Line breaks are what's left (see UnnestStringOpExecSpec), so the message says so rather
  // than claiming nothing is.
  it should "reject a pattern that matches every character but line breaks, naming what's left" in {
    List(".", "[^\\n]", ".+").foreach { pattern =>
      withClue(s"pattern '$pattern': ") {
        compileError(pattern).getOrElse("") should include(
          "matches every character except line breaks, so only line breaks would be left"
        )
      }
    }
  }

  it should "suggest the escape for a lone metacharacter that matches everything" in {
    compileError(".").getOrElse("") should include("\\.")
  }

  it should "reject an empty delimiter" in {
    compileError("").getOrElse("") should include("cannot be empty")
  }

  it should "accept useful patterns that use Java-only syntax" in {
    List("\\p{L}+", "(?i)and", ",++", "\\h*,\\h*", "\\Q|\\E", "\\s+").foreach { pattern =>
      withClue(s"pattern '$pattern': ") {
        compileError(pattern) shouldBe None
      }
    }
  }

  it should "accept every preset the delimiter picker offers" in {
    val input = Schema().add(new Attribute("tags", AttributeType.STRING))
    List(",", "\\t", "\t", ";", "\\|", "\\s+", "\\n").foreach { preset =>
      withClue(s"preset '$preset': ") {
        val op = newDesc(preset, "tags", "tag")
        val physical = op.getPhysicalOp(workflowId, executionId)
        physical.propagateSchema.func(Map(op.operatorInfo.inputPorts.head.id -> input))
      }
    }
  }

  "UnnestStringOpDesc json schema" should "render the delimiter as the regex delimiter picker" in {
    val formlyConfig = OperatorMetadataGenerator
      .generateOperatorJsonSchema(classOf[UnnestStringOpDesc])
      .path("properties")
      .path("Delimiter")
      .path("widget")
      .path("formlyConfig")
    formlyConfig.path("type").asText() shouldBe "delimiter"
    formlyConfig.path("props").path("delimiterMode").asText() shouldBe "regex"
  }

  // A hole widens an integer column to float, so the split would see "6.0" where
  // the engine splits "6". A real DOUBLE holding 6.0 looks the same and keeps its
  // point, so only the declared type can decide.
  "UnnestStringOpDesc.generateStandaloneCode" should
    "narrow a column the schema declares INTEGER before rendering it" in {
    val op = newDesc(",", "n", "piece")
    val schemas = Map(PortIdentity(0) -> Schema().add(new Attribute("n", AttributeType.INTEGER)))
    op.generateStandaloneCode(schemas) should include(
      """_texera_cast_string(out1df["n"].astype("Int64"))"""
    )
  }

  it should "leave a DOUBLE column its decimal point" in {
    val op = newDesc(",", "n", "piece")
    val schemas = Map(PortIdentity(0) -> Schema().add(new Attribute("n", AttributeType.DOUBLE)))
    op.generateStandaloneCode(schemas) should include("""_texera_cast_string(out1df["n"])""")
  }

  it should "read the column as it arrives when no schema is given" in {
    newDesc(",", "n", "piece").generateStandaloneCode() should include(
      """_texera_cast_string(out1df["n"])"""
    )
  }

  "UnnestStringOpDesc" should "round-trip its fields through the polymorphic base" in {
    val restored =
      objectMapper.readValue(
        objectMapper.writeValueAsString(newDesc(";", "csv", "item")),
        classOf[LogicalOp]
      )
    restored shouldBe a[UnnestStringOpDesc]
    val u = restored.asInstanceOf[UnnestStringOpDesc]
    u.delimiter shouldBe ";"
    u.attribute shouldBe "csv"
    u.resultAttribute shouldBe "item"
  }
}
