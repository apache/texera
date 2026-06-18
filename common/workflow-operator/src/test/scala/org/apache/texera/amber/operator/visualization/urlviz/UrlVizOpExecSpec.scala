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

package org.apache.texera.amber.operator.visualization.urlviz

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple, TupleLike}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

class UrlVizOpExecSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Fixtures
  // ---------------------------------------------------------------------------

  private val attr = new Attribute("url", AttributeType.STRING)
  private val schema: Schema = Schema().add(attr)
  private def tuple(url: String): Tuple =
    Tuple.builder(schema).add(attr, url).build()

  /**
    * Build a descriptor JSON for `UrlVizOpExec`. The production class
    * declares `urlContentAttrName` as a `val` initialized to `""`; the
    * Jackson-`module-no-ctor-deser` module bypasses the val and writes
    * to the underlying field via reflection, so the spec can supply
    * any attribute name through the JSON wire-key.
    *
    * LogicalOp carries `@JsonTypeInfo(property = "operatorType")`, so
    * the JSON must include the `operatorType` discriminator (value
    * `"URLVisualizer"` per LogicalOp's `@JsonSubTypes` table).
    */
  private def descJson(urlAttr: String): String = {
    val node = objectMapper.createObjectNode()
    node.put("operatorType", "URLVisualizer")
    node.put("urlContentAttrName", urlAttr)
    node.toString
  }

  private def runSingle(exec: UrlVizOpExec, t: Tuple): String = {
    val out = exec.processTuple(t, port = 0).toList
    assert(out.size == 1, s"expected one emission, got: $out")
    out.head match {
      case tupleLike: TupleLike =>
        val fields = tupleLike.getFields
        assert(
          fields.size == 1,
          s"expected exactly one field on the emitted TupleLike, got: $fields"
        )
        val field = fields.head
        assert(
          field.isInstanceOf[String],
          s"expected the field to be a String, got ${field.getClass.getName}: $field"
        )
        field.asInstanceOf[String]
    }
  }

  // ---------------------------------------------------------------------------
  // Generated HTML contents — pinned via canonical substrings
  // ---------------------------------------------------------------------------

  "UrlVizOpExec.processTuple" should "emit an HTML iframe referencing the input URL" in {
    val exec = new UrlVizOpExec(descJson("url"))
    val html = runSingle(exec, tuple("https://example.invalid/page"))
    assert(html.contains("<!DOCTYPE html>"))
    assert(
      html.contains("<iframe src=\"https://example.invalid/page\""),
      s"expected iframe src to embed the input URL, got: $html"
    )
  }

  it should "include `frameborder=\"0\"` so the iframe renders without a border" in {
    val exec = new UrlVizOpExec(descJson("url"))
    val html = runSingle(exec, tuple("about:blank"))
    assert(html.contains("frameborder=\"0\""))
  }

  it should
    "include the full-viewport sizing style `height:100vh; width:100%; border:none`" in {
    val exec = new UrlVizOpExec(descJson("url"))
    val html = runSingle(exec, tuple("about:blank"))
    assert(html.contains("height:100vh"))
    assert(html.contains("width:100%"))
    assert(html.contains("border:none"))
  }

  it should "interpolate distinct URLs into distinct outputs" in {
    val exec = new UrlVizOpExec(descJson("url"))
    val first = runSingle(exec, tuple("https://example.com/a"))
    val second = runSingle(exec, tuple("https://example.com/b"))
    assert(first.contains("https://example.com/a"))
    assert(second.contains("https://example.com/b"))
    assert(first != second)
  }

  it should "produce exactly one emission per input tuple" in {
    val exec = new UrlVizOpExec(descJson("url"))
    val iter = exec.processTuple(tuple("https://x.invalid/"), port = 0)
    assert(iter.hasNext)
    iter.next()
    assert(!iter.hasNext)
  }

  // ---------------------------------------------------------------------------
  // Descriptor parse failure surfaces during construction
  // ---------------------------------------------------------------------------

  "UrlVizOpExec construction" should
    "throw on malformed descriptor JSON" in {
    intercept[com.fasterxml.jackson.core.JsonProcessingException] {
      new UrlVizOpExec("{not valid")
    }
  }
}
