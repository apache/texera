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
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class UnnestStringOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val tupleSchema: Schema = Schema()
    .add(new Attribute("field1", AttributeType.STRING))
    .add(new Attribute("field2", AttributeType.INTEGER))
    .add(new Attribute("field3", AttributeType.STRING))

  val tuple: Tuple = Tuple
    .builder(tupleSchema)
    .add(new Attribute("field1", AttributeType.STRING), "a-b-c")
    .add(new Attribute("field2", AttributeType.INTEGER), 1)
    .add(new Attribute("field3", AttributeType.STRING), "a")
    .build()

  var opExec: UnnestStringOpExec = _
  var opDesc: UnnestStringOpDesc = _
  var outputSchema: Schema = _
  before {
    opDesc = new UnnestStringOpDesc()
    opDesc.attribute = "field1"
    opDesc.delimiter = "-"
    opDesc.resultAttribute = "split"
  }

  it should "open" in {
    opDesc.attribute = "field1"
    opDesc.delimiter = "-"
    opExec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
    opExec.open()
    assert(opExec.flatMapFunc != null)
  }

  it should "split value in the given attribute and output the split result in the result attribute, one for each tuple" in {
    opDesc.attribute = "field1"
    opDesc.delimiter = "-"
    opExec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
    opExec.open()
    val processedTuple = opExec
      .processTuple(tuple, 0)
      .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
    assert(processedTuple.next().getField("split").equals("a"))
    assert(processedTuple.next().getField("split").equals("b"))
    assert(processedTuple.next().getField("split").equals("c"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("split"))
    opExec.close()
  }

  it should "generate the correct tuple when there is no delimiter in the value" in {
    opDesc.attribute = "field3"
    opDesc.delimiter = "-"
    opExec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
    opExec.open()
    val processedTuple = opExec
      .processTuple(tuple, 0)
      .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
    assert(processedTuple.next().getField("split").equals("a"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("split"))
    opExec.close()
  }

  it should "only contain split results that are not null" in {
    opDesc.attribute = "field1"
    opDesc.delimiter = "/"
    opExec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
    val tuple: Tuple = Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), "//a//b/")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(new Attribute("field3", AttributeType.STRING), "a")
      .build()

    opExec.open()
    val processedTuple = opExec
      .processTuple(tuple, 0)
      .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
    assert(processedTuple.next().getField("split").equals("a"))
    assert(processedTuple.next().getField("split").equals("b"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("split"))
    opExec.close()
  }

  it should "produce no rows when the attribute is empty" in {
    opDesc.attribute = "field1"
    opDesc.delimiter = "-"
    opExec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    // A blank CSV cell arrives as null. This used to throw a NullPointerException on
    // the toString instead of unnesting to nothing.
    val tuple: Tuple = Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), null)
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(new Attribute("field3", AttributeType.STRING), "a")
      .build()

    opExec.open()
    assert(opExec.processTuple(tuple, 0).isEmpty)
    opExec.close()
  }

  it should "split by regex delimiter" in {
    opDesc.attribute = "field1"
    opDesc.delimiter = "<\\d*>"
    opExec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    outputSchema = opDesc.getExternalOutputSchemas(Map(PortIdentity() -> tupleSchema)).values.head
    val tuple: Tuple = Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), "<>a<1>b<12>")
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(new Attribute("field3", AttributeType.STRING), "a")
      .build()

    opExec.open()
    val processedTuple = opExec
      .processTuple(tuple, 0)
      .map(tupleLike => tupleLike.asInstanceOf[SchemaEnforceable].enforceSchema(outputSchema))
    assert(processedTuple.next().getField("split").equals("a"))
    assert(processedTuple.next().getField("split").equals("b"))
    assertThrows[java.util.NoSuchElementException](processedTuple.next().getField("split"))
    opExec.close()
  }

  // Runs the operator over one value and returns the pieces it emits.
  private def unnest(delimiter: String, value: String): List[String] = {
    opDesc.attribute = "field1"
    opDesc.delimiter = delimiter
    val exec = new UnnestStringOpExec(objectMapper.writeValueAsString(opDesc))
    // Built directly, not propagated: schema propagation now rejects the degenerate patterns
    // (`.`, `|`) whose runtime behavior the tests below pin.
    val schema = tupleSchema.add(new Attribute("split", AttributeType.STRING))
    val input = Tuple
      .builder(tupleSchema)
      .add(new Attribute("field1", AttributeType.STRING), value)
      .add(new Attribute("field2", AttributeType.INTEGER), 1)
      .add(new Attribute("field3", AttributeType.STRING), "a")
      .build()
    exec.open()
    val pieces = exec
      .processTuple(input, 0)
      .map(_.asInstanceOf[SchemaEnforceable].enforceSchema(schema).getField[String]("split"))
      .toList
    exec.close()
    pieces
  }

  // The presets the delimiter picker offers in regex mode, exactly as it stores them.
  "UnnestStringOpExec with the picker's presets" should "split on a comma" in {
    assert(unnest(",", "a,b,c") == List("a", "b", "c"))
  }

  it should "split on a tab written as the regex escape" in {
    assert(unnest("\\t", "a\tb\tc") == List("a", "b", "c"))
  }

  it should "split on a tab stored as the literal character by an older workflow" in {
    assert(unnest("\t", "a\tb\tc") == List("a", "b", "c"))
  }

  it should "split on a semicolon" in {
    assert(unnest(";", "a;b;c") == List("a", "b", "c"))
  }

  it should "split on an escaped pipe" in {
    assert(unnest("\\|", "a|b|c") == List("a", "b", "c"))
  }

  it should "split on any run of whitespace" in {
    assert(unnest("\\s+", "a  b\t c\nd") == List("a", "b", "c", "d"))
  }

  it should "split on a new line written as the regex escape" in {
    assert(unnest("\\n", "a\nb\nc") == List("a", "b", "c"))
  }

  "UnnestStringOpExec with a custom regex" should "split on a character class" in {
    assert(unnest("[,;]", "a,b;c") == List("a", "b", "c"))
  }

  it should "absorb the spaces around a comma" in {
    assert(unnest("\\s*,\\s*", "a , b,  c") == List("a", "b", "c"))
  }

  it should "split on a multi-character delimiter" in {
    assert(unnest("::", "a::b::c") == List("a", "b", "c"))
  }

  it should "split on an escaped dot and keep the text between dots" in {
    assert(unnest("\\.", "a.b.c") == List("a", "b", "c"))
  }

  it should "drop everything for an unescaped dot, which matches every character" in {
    // Why the operator rejects it at compile time: every piece is empty.
    assert(unnest(".", "a.b.c") == List())
  }

  it should "leave only the line breaks of a multi-line value for an unescaped dot" in {
    assert(unnest(".", "ab\ncd\r\ne") == List("\n", "\r\n"))
  }

  it should "leave only the line breaks of a multi-line value for anything but a line feed" in {
    assert(unnest("[^\\n]", "ab\ncd") == List("\n"))
  }

  it should "split every character apart for a bare pipe, the empty alternation" in {
    // The operator rejects this pattern at compile time; this pins what it would have done.
    assert(unnest("|", "abc") == List("a", "b", "c"))
  }

  it should "split on a non-ASCII delimiter" in {
    assert(unnest("•", "a•b•c") == List("a", "b", "c"))
  }

  it should "accept Java-only syntax such as a possessive quantifier" in {
    assert(unnest(",++", "a,,b,c") == List("a", "b", "c"))
  }
}
