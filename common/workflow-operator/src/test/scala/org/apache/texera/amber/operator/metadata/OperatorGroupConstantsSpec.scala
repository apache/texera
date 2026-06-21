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

package org.apache.texera.amber.operator.metadata

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OperatorGroupConstantsSpec extends AnyFlatSpec with Matchers {

  import OperatorGroupConstants._

  "OperatorGroupConstants" should "pin the canonical group-name string values" in {
    INPUT_GROUP shouldBe "Data Input"
    SEARCH_GROUP shouldBe "Search"
    CLEANING_GROUP shouldBe "Data Cleaning"
    JOIN_GROUP shouldBe "Join"
    SET_GROUP shouldBe "Set"
    AGGREGATE_GROUP shouldBe "Aggregate"
    SORT_GROUP shouldBe "Sort"
    UTILITY_GROUP shouldBe "Utilities"
    VISUALIZATION_GROUP shouldBe "Visualization"
    VISUALIZATION_BASIC_GROUP shouldBe "Basic"
    VISUALIZATION_SCIENTIFIC_GROUP shouldBe "Scientific"
    VISUALIZATION_FINANCIAL_GROUP shouldBe "Financial"
    CONTROL_GROUP shouldBe "Control Block"
  }

  "OperatorGroupOrderList" should "start at Data Input, contain Visualization, and place Control Block last" in {
    val names = OperatorGroupOrderList.map(_.groupName)
    names.head shouldBe INPUT_GROUP
    names.last shouldBe CONTROL_GROUP
    names should contain(VISUALIZATION_GROUP)
  }

  it should "nest the relational subgroups (Join/Set/Aggregate/Sort) under Data Cleaning" in {
    val cleaning = OperatorGroupOrderList
      .find(_.groupName == CLEANING_GROUP)
      .getOrElse(fail("Data Cleaning group missing from OperatorGroupOrderList"))
    cleaning.children.map(_.groupName) shouldBe List(
      JOIN_GROUP,
      SET_GROUP,
      AGGREGATE_GROUP,
      SORT_GROUP
    )
  }

  it should "nest the visualization subgroups under Visualization (in panel order)" in {
    val viz = OperatorGroupOrderList
      .find(_.groupName == VISUALIZATION_GROUP)
      .getOrElse(fail("Visualization group missing from OperatorGroupOrderList"))
    viz.children.map(_.groupName) shouldBe List(
      VISUALIZATION_BASIC_GROUP,
      VISUALIZATION_STATISTICAL_GROUP,
      VISUALIZATION_SCIENTIFIC_GROUP,
      VISUALIZATION_FINANCIAL_GROUP,
      VISUALIZATION_MEDIA_GROUP,
      VISUALIZATION_ADVANCED_GROUP
    )
  }
}
