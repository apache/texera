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
import org.apache.texera.amber.operator.dictionary.MatchingType
import org.apache.texera.amber.operator.filter.ComparisonType
import org.apache.texera.amber.operator.hashJoin.JoinType
import org.apache.texera.amber.operator.intervalJoin.TimeIntervalType
import org.apache.texera.amber.operator.machineLearning.Scorer.{
  classificationMetricsFnc,
  regressionMetricsFnc
}
import org.apache.texera.amber.operator.source.apis.reddit.RedditSourceOperatorFunction
import org.apache.texera.amber.operator.source.fetcher.DecodingMethod
import org.apache.texera.amber.operator.source.scan.{FileAttributeType, FileDecodingMethod}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag

class OperatorEnumsSpec extends AnyFlatSpec with Matchers {

  private def checkEnum[E <: Enum[E]: ClassTag](
      expectedMappings: Seq[(E, String)],
      getWireName: E => String,
      mappingTestDesc: String = "map each constant to its wire name via getName",
      roundTripTestDesc: String = "round-trip through Jackson using its wire name",
      toStringMatchesWire: Boolean = false
  ): Unit = {
    val enumClass = implicitly[ClassTag[E]].runtimeClass.asInstanceOf[Class[E]]
    val enumName = enumClass.getSimpleName

    s"$enumName" should mappingTestDesc in {
      val allConstants = enumClass.getEnumConstants
      allConstants should have length expectedMappings.length
      expectedMappings.foreach {
        case (constant, expectedWireName) =>
          getWireName(constant) shouldBe expectedWireName
          if (toStringMatchesWire) {
            constant.toString shouldBe expectedWireName
          }
      }
    }

    s"$enumName" should roundTripTestDesc in {
      expectedMappings.foreach {
        case (constant, wireName) =>
          objectMapper.writeValueAsString(constant) shouldBe s""""$wireName""""
          objectMapper.readValue(s""""$wireName"""", enumClass) shouldBe constant
      }
    }
  }

  checkEnum[AggregationFunction](
    Seq(
      AggregationFunction.SUM -> "sum",
      AggregationFunction.COUNT -> "count",
      AggregationFunction.AVERAGE -> "average",
      AggregationFunction.MIN -> "min",
      AggregationFunction.MAX -> "max",
      AggregationFunction.CONCAT -> "concat"
    ),
    _.getName
  )

  checkEnum[MatchingType](
    Seq(
      MatchingType.SCANBASED -> "Scan",
      MatchingType.SUBSTRING -> "Substring",
      MatchingType.CONJUNCTION_INDEXBASED -> "Conjunction"
    ),
    _.getName
  )

  checkEnum[ComparisonType](
    Seq(
      ComparisonType.EQUAL_TO -> "=",
      ComparisonType.GREATER_THAN -> ">",
      ComparisonType.GREATER_THAN_OR_EQUAL_TO -> ">=",
      ComparisonType.LESS_THAN -> "<",
      ComparisonType.LESS_THAN_OR_EQUAL_TO -> "<=",
      ComparisonType.NOT_EQUAL_TO -> "!=",
      ComparisonType.IS_NULL -> "is null",
      ComparisonType.IS_NOT_NULL -> "is not null"
    ),
    _.getName,
    mappingTestDesc = "map each constant to its wire symbol via getName",
    roundTripTestDesc = "round-trip through Jackson using its symbol"
  )

  "ComparisonType.fromString" should "resolve symbols case-insensitively" in {
    ComparisonType.fromString(">=") shouldBe ComparisonType.GREATER_THAN_OR_EQUAL_TO
    ComparisonType.fromString("IS NULL") shouldBe ComparisonType.IS_NULL
    ComparisonType.fromString("is not null") shouldBe ComparisonType.IS_NOT_NULL
  }

  it should "reject an unknown symbol" in {
    intercept[IllegalArgumentException](ComparisonType.fromString("≈"))
  }

  checkEnum[JoinType](
    Seq(
      JoinType.INNER -> "inner",
      JoinType.LEFT_OUTER -> "left outer",
      JoinType.RIGHT_OUTER -> "right outer",
      JoinType.FULL_OUTER -> "full outer"
    ),
    _.getJoinType,
    mappingTestDesc = "map each constant to its wire value via getJoinType",
    roundTripTestDesc = "round-trip through Jackson using its wire value"
  )

  checkEnum[TimeIntervalType](
    Seq(
      TimeIntervalType.YEAR -> "year",
      TimeIntervalType.MONTH -> "month",
      TimeIntervalType.DAY -> "day",
      TimeIntervalType.HOUR -> "hour",
      TimeIntervalType.MINUTE -> "minute",
      TimeIntervalType.SECOND -> "second"
    ),
    _.getName,
    mappingTestDesc = "map each constant to its wire name (also its toString)",
    toStringMatchesWire = true
  )

  checkEnum[classificationMetricsFnc](
    Seq(
      classificationMetricsFnc.accuracy -> "Accuracy",
      classificationMetricsFnc.precisionScore -> "Precision Score",
      classificationMetricsFnc.recallScore -> "Recall Score",
      classificationMetricsFnc.f1Score -> "F1 Score"
    ),
    _.getName,
    mappingTestDesc = "map each constant to its wire value",
    roundTripTestDesc = "round-trip through Jackson using its wire value"
  )

  checkEnum[regressionMetricsFnc](
    Seq(
      regressionMetricsFnc.mse -> "MSE",
      regressionMetricsFnc.rmse -> "RMSE",
      regressionMetricsFnc.mae -> "MAE",
      regressionMetricsFnc.r2 -> "R2"
    ),
    _.getName,
    mappingTestDesc = "map each constant to its wire value",
    roundTripTestDesc = "round-trip through Jackson using its wire value"
  )

  checkEnum[RedditSourceOperatorFunction](
    Seq(
      RedditSourceOperatorFunction.None -> "none",
      RedditSourceOperatorFunction.Controversial -> "controversial",
      RedditSourceOperatorFunction.Gilded -> "gilded",
      RedditSourceOperatorFunction.Hot -> "hot",
      RedditSourceOperatorFunction.New -> "new",
      RedditSourceOperatorFunction.Rising -> "rising",
      RedditSourceOperatorFunction.Top -> "top"
    ),
    _.getName,
    mappingTestDesc = "map each constant to its wire value",
    roundTripTestDesc = "round-trip through Jackson using its wire value"
  )

  checkEnum[DecodingMethod](
    Seq(
      DecodingMethod.UTF_8 -> "UTF-8",
      DecodingMethod.RAW_BYTES -> "RAW BYTES"
    ),
    _.getName,
    mappingTestDesc = "map each constant to its wire value",
    roundTripTestDesc = "round-trip through Jackson using its wire value"
  )

  checkEnum[FileAttributeType](
    Seq(
      FileAttributeType.STRING -> "string",
      FileAttributeType.SINGLE_STRING -> "single string",
      FileAttributeType.INTEGER -> "integer",
      FileAttributeType.LONG -> "long",
      FileAttributeType.DOUBLE -> "double",
      FileAttributeType.BOOLEAN -> "boolean",
      FileAttributeType.TIMESTAMP -> "timestamp",
      FileAttributeType.BINARY -> "binary",
      FileAttributeType.LARGE_BINARY -> "large binary"
    ),
    _.getName,
    mappingTestDesc = "map each constant to its wire name (also its toString)",
    toStringMatchesWire = true
  )

  "FileAttributeType.getType" should "map to the corresponding core AttributeType" in {
    FileAttributeType.STRING.getType shouldBe AttributeType.STRING
    FileAttributeType.SINGLE_STRING.getType shouldBe AttributeType.STRING
    FileAttributeType.INTEGER.getType shouldBe AttributeType.INTEGER
    FileAttributeType.LONG.getType shouldBe AttributeType.LONG
    FileAttributeType.DOUBLE.getType shouldBe AttributeType.DOUBLE
    FileAttributeType.BOOLEAN.getType shouldBe AttributeType.BOOLEAN
    FileAttributeType.TIMESTAMP.getType shouldBe AttributeType.TIMESTAMP
    FileAttributeType.BINARY.getType shouldBe AttributeType.BINARY
    FileAttributeType.LARGE_BINARY.getType shouldBe AttributeType.LARGE_BINARY
  }

  "FileAttributeType.isSingle" should "hold only for the single-value types" in {
    FileAttributeType.SINGLE_STRING.isSingle shouldBe true
    FileAttributeType.BINARY.isSingle shouldBe true
    FileAttributeType.LARGE_BINARY.isSingle shouldBe true
    FileAttributeType.STRING.isSingle shouldBe false
    FileAttributeType.INTEGER.isSingle shouldBe false
  }

  checkEnum[FileDecodingMethod](
    Seq(
      FileDecodingMethod.UTF_8 -> "UTF_8",
      FileDecodingMethod.UTF_16 -> "UTF_16",
      FileDecodingMethod.ASCII -> "US_ASCII"
    ),
    _.getName,
    mappingTestDesc = "map each constant to its wire value",
    roundTripTestDesc = "round-trip through Jackson using its wire value"
  )
}
