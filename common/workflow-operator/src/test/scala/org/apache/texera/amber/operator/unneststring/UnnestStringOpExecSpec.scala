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

import org.apache.texera.amber.core.tuple._
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class UnnestStringOpExecSpec extends AnyFlatSpec {
  private val tupleSchema: Schema = Schema()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.STRING))

  private val tuple: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "a-b-c")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(new Attribute("field3", AttributeType.STRING), "a")
    .build()

  private def unnest(
      delimiter: String,
      attribute: String = "field1",
      targetTuple: Tuple = tuple
  ): List[String] = {
    val opDesc = new UnnestStringOpDesc()
    opDesc.attribute = attribute
    opDesc.delimiter = delimiter
    opDesc.resultAttribute = "split"
    val exec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    val schema = tupleSchema.add(new Attribute("split", AttributeType.STRING))
    exec.open()
    try {
      exec
        .processTuple(targetTuple, 0)
        .map(_.asInstanceOf[SchemaEnforceable].enforceSchema(schema).getField[String]("split"))
        .toList
    } finally {
      exec.close()
    }
  }

  // Runs the operator over one value and returns the pieces it emits.
  private def unnestValue(delimiter: String, value: String): List[String] = {
    val input = Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), value)
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(new Attribute("field3", AttributeType.STRING), "a")
      .build()
    unnest(delimiter, "field1", input)
  }

  it should "open" in {
    val opDesc = new UnnestStringOpDesc()
    opDesc.attribute = "field1"
    opDesc.delimiter = "-"
    opDesc.resultAttribute = "split"
    val opExec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    try {
      assert(opExec.flatMapFunc != null)
    } finally {
      opExec.close()
    }
  }

  it should "split value in the given attribute and output the split result in the result attribute, one for each tuple" in {
    assert(unnest("-") == List("a", "b", "c"))
  }

  it should "generate the correct tuple when there is no delimiter in the value" in {
    assert(unnest("-", attribute = "field3") == List("a"))
  }

  it should "only contain split results that are not null" in {
    val customTuple = Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), "//a//b/")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(new Attribute("field3", AttributeType.STRING), "a")
      .build()
    assert(unnest("/", targetTuple = customTuple) == List("a", "b"))
  }

  it should "produce no rows when the attribute is empty" in {
    // A blank CSV cell arrives as null. This used to throw a NullPointerException on
    // the toString instead of unnesting to nothing.
    val customTuple = Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), null)
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(new Attribute("field3", AttributeType.STRING), "a")
      .build()
    assert(unnest("-", targetTuple = customTuple).isEmpty)
  }

  it should "split by regex delimiter" in {
    val customTuple = Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), "<>a<1>b<12>")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(new Attribute("field3", AttributeType.STRING), "a")
      .build()
    assert(unnest("<\\d*>", targetTuple = customTuple) == List("a", "b"))
  }

  // The presets the delimiter picker offers in regex mode, exactly as it stores them.
  "UnnestStringOpExec with the picker's presets" should "split on a comma" in {
    assert(unnestValue(",", "a,b,c") == List("a", "b", "c"))
  }

  it should "split on a tab written as the regex escape" in {
    assert(unnestValue("\\t", "a\tb\tc") == List("a", "b", "c"))
  }

  it should "split on a tab stored as the literal character by an older workflow" in {
    assert(unnestValue("\t", "a\tb\tc") == List("a", "b", "c"))
  }

  it should "split on a semicolon" in {
    assert(unnestValue(";", "a;b;c") == List("a", "b", "c"))
  }

  it should "split on an escaped pipe" in {
    assert(unnestValue("\\|", "a|b|c") == List("a", "b", "c"))
  }

  it should "split on any run of whitespace" in {
    assert(unnestValue("\\s+", "a  b\t c\nd") == List("a", "b", "c", "d"))
  }

  it should "split on a new line written as the regex escape" in {
    assert(unnestValue("\\n", "a\nb\nc") == List("a", "b", "c"))
  }

  "UnnestStringOpExec with a custom regex" should "split on a character class" in {
    assert(unnestValue("[,;]", "a,b;c") == List("a", "b", "c"))
  }

  it should "absorb the spaces around a comma" in {
    assert(unnestValue("\\s*,\\s*", "a , b,  c") == List("a", "b", "c"))
  }

  it should "split on a multi-character delimiter" in {
    assert(unnestValue("::", "a::b::c") == List("a", "b", "c"))
  }

  it should "split on an escaped dot and keep the text between dots" in {
    assert(unnestValue("\\.", "a.b.c") == List("a", "b", "c"))
  }

  it should "drop everything for an unescaped dot, which matches every character" in {
    // Why the operator rejects it at compile time: every piece is empty.
    assert(unnestValue(".", "a.b.c") == List())
  }

  it should "leave only the line breaks of a multi-line value for an unescaped dot" in {
    assert(unnestValue(".", "ab\ncd\r\ne") == List("\n", "\r\n"))
  }

  it should "leave only the line breaks of a multi-line value for anything but a line feed" in {
    assert(unnestValue("[^\\n]", "ab\ncd") == List("\n"))
  }

  it should "split every character apart for a bare pipe, the empty alternation" in {
    // The operator rejects this pattern at compile time; this pins what it would have done.
    assert(unnestValue("|", "abc") == List("a", "b", "c"))
  }

  it should "split on a non-ASCII delimiter" in {
    assert(unnestValue("•", "a•b•c") == List("a", "b", "c"))
  }

  it should "accept Java-only syntax such as a possessive quantifier" in {
    assert(unnestValue(",++", "a,,b,c") == List("a", "b", "c"))
  }
}
