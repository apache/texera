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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class KeywordSearchOpExecSpec extends AnyFlatSpec with BeforeAndAfter {
  val inputPort: Int = 0
  val opDesc: KeywordSearchOpDesc = new KeywordSearchOpDesc()

  val schema: Schema = Schema()
    .add(new Attribute("text", AttributeType.STRING))

  def createTuple(text: String): Tuple = {
    Tuple
      .builder(schema)
      .add(new Attribute("text", AttributeType.STRING), text)
      .build()
  }

  val testData: List[Tuple] = List(
    createTuple("3 stars"),
    createTuple("4 stars"),
    createTuple("Trump"),
    createTuple("Trump Biden"),
    createTuple("hello"),
    createTuple("the name"),
    createTuple("an eye"),
    createTuple("to you"),
    createTuple("Twitter"),
    createTuple("안녕하세요"),
    createTuple("你好"),
    createTuple("_!@,-"),
    createTuple("everything was absolutely perfect.")
  )

  private def searchMatches(keyword: String, isCaseSensitive: Boolean = false): List[String] = {
    opDesc.attribute = "text"
    opDesc.keyword = keyword
    opDesc.isCaseSensitive = isCaseSensitive
    val opExec = new KeywordSearchOpExec(objectMapper.writeValueAsString(opDesc))
    opExec.open()
    try {
      testData
        .filter(t => opExec.processTuple(t, inputPort).hasNext)
        .map(_.getField[String]("text"))
    } finally {
      opExec.close()
      opDesc.isCaseSensitive = false
    }
  }

  it should "find exact match with single number" in {
    assert(searchMatches("3") == List("3 stars"))
  }

  it should "find exact phrase match" in {
    assert(searchMatches("\"3 stars\"") == List("3 stars"))
  }

  it should "find all occurrences of Trump" in {
    assert(searchMatches("Trump").toSet == Set("Trump Biden", "Trump"))
  }

  it should "find all occurrences of Biden" in {
    assert(searchMatches("Biden") == List("Trump Biden"))
  }

  it should "find records containing both Trump AND Biden" in {
    assert(searchMatches("Trump AND Biden") == List("Trump Biden"))
  }

  it should "find no matches for exact phrase 'Trump AND Biden'" in {
    assert(searchMatches("\"Trump AND Biden\"").isEmpty)
  }

  it should "find no matches for partial word 'ell'" in {
    assert(searchMatches("ell").isEmpty)
  }

  it should "find exact match for word 'the'" in {
    assert(searchMatches("the") == List("the name"))
  }

  it should "find exact match for word 'an'" in {
    assert(searchMatches("an") == List("an eye"))
  }

  it should "find exact match for word 'to'" in {
    assert(searchMatches("to") == List("to you"))
  }

  it should "find case-insensitive match for 'twitter'" in {
    assert(searchMatches("twitter") == List("Twitter"))
  }

  it should "find exact match for Korean text '안녕하세요'" in {
    assert(searchMatches("안녕하세요") == List("안녕하세요"))
  }

  it should "find exact match for Chinese text '你好'" in {
    assert(searchMatches("你好") == List("你好"))
  }

  it should "find no matches for special character '@'" in {
    assert(searchMatches("@").isEmpty)
  }

  it should "find exact match for special characters '_!@,-'" in {
    assert(searchMatches("_!@,-").isEmpty)
  }

  it should "not match 'twitter' against 'Twitter' when case-sensitive" in {
    assert(searchMatches("twitter", isCaseSensitive = true).isEmpty)
  }

  it should "match 'Twitter' against 'Twitter' when case-sensitive" in {
    assert(searchMatches("Twitter", isCaseSensitive = true) == List("Twitter"))
  }

  it should "match a word adjacent to punctuation when case-sensitive" in {
    // Regression test: case sensitivity must change only case handling, not
    // punctuation -- "perfect" should still match "...perfect." (word-boundary split).
    assert(
      searchMatches("perfect", isCaseSensitive = true) == List("everything was absolutely perfect.")
    )
  }
}
