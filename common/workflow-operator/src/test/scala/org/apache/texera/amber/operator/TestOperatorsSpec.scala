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

package org.apache.texera.amber.operator

import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.aggregate.AggregationFunction
import org.scalatest.flatspec.AnyFlatSpec

class TestOperatorsSpec extends AnyFlatSpec {

  "TestOperators.getCsvScanOpDesc" should "build a resolved CSV desc honoring the header flag" in {
    val op = TestOperators.getCsvScanOpDesc(TestOperators.CountrySalesSmallCsvPath, header = true)
    assert(op.hasHeader)
    assert(op.customDelimiter.contains(","))
    assert(op.fileName.isDefined)
    assert(op.fileResolved())
    val headerless =
      TestOperators.getCsvScanOpDesc(
        TestOperators.CountrySalesHeaderlessSmallCsvPath,
        header = false,
        multiLine = true
      )
    assert(!headerless.hasHeader)
  }

  "TestOperators CSV factories" should "resolve their backing files" in {
    assert(!TestOperators.headerlessSmallCsvScanOpDesc().hasHeader)
    assert(!TestOperators.headerlessSmallMultiLineDataCsvScanOpDesc().hasHeader)
    assert(TestOperators.smallCsvScanOpDesc().hasHeader)
    assert(TestOperators.mediumCsvScanOpDesc().hasHeader)
    assert(TestOperators.smallCsvScanOpDesc().fileResolved())
  }

  "TestOperators.getJSONLScanOpDesc" should "build a resolved JSONL desc honoring the flatten flag" in {
    val op = TestOperators.getJSONLScanOpDesc(TestOperators.smallJsonLPath)
    assert(!op.flatten)
    assert(op.fileResolved())
    val flattened = TestOperators.getJSONLScanOpDesc(TestOperators.mediumJsonLPath, flatten = true)
    assert(flattened.flatten)
  }

  "TestOperators JSONL factories" should "resolve their backing files" in {
    assert(!TestOperators.smallJSONLScanOpDesc().flatten)
    assert(TestOperators.mediumFlattenJSONLScanOpDesc().flatten)
    assert(TestOperators.smallJSONLScanOpDesc().fileResolved())
  }

  "TestOperators.joinOpDesc" should "set the build and probe attribute names" in {
    val op = TestOperators.joinOpDesc("build_col", "probe_col")
    assert(op.buildAttributeName == "build_col")
    assert(op.probeAttributeName == "probe_col")
  }

  "TestOperators.keywordSearchOpDesc" should "set the attribute and keyword" in {
    val op = TestOperators.keywordSearchOpDesc("text", "hello")
    assert(op.attribute == "text")
    assert(op.keyword == "hello")
  }

  "TestOperators.aggregateAndGroupByDesc" should "wire a single aggregation with group-by keys" in {
    val op = TestOperators.aggregateAndGroupByDesc("price", AggregationFunction.SUM, List("region"))
    assert(op.aggregations.length == 1)
    assert(op.aggregations.head.aggFunction == AggregationFunction.SUM)
    assert(op.aggregations.head.attribute == "price")
    assert(op.aggregations.head.resultAttribute == "aggregate-result")
    assert(op.groupByKeys == List("region"))
  }

  "TestOperators.asterixDBSourceOpDesc" should "build the AsterixDB desc without contacting a server" in {
    val op = TestOperators.asterixDBSourceOpDesc()
    assert(op.host == "ipubmed4.ics.uci.edu")
    assert(op.port == "default")
    assert(op.database == "twitter")
    assert(op.table == "ds_tweet")
    assert(op.limit.contains(1000L))
  }

  "TestOperators.pythonOpDesc" should "build a python UDF desc" in {
    val op = TestOperators.pythonOpDesc()
    assert(op.workers == 1)
    assert(op.retainInputColumns)
    assert(op.code.contains("ProcessTupleOperator"))
    assert(op.code.contains("process_tuple"))
  }

  "TestOperators.pythonSourceOpDesc" should "build a python source UDF desc interpolating the tuple count" in {
    val op = TestOperators.pythonSourceOpDesc(5)
    assert(op.workers == 1)
    assert(op.columns.length == 1)
    assert(op.columns.head.getName == "field_1")
    assert(op.columns.head.getType == AttributeType.STRING)
    assert(op.code.contains("range(5)"))
  }
}
