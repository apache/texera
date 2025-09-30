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

package edu.uci.ics.amber.operator.visualization.barChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class BarChartOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var opDesc: BarChartOpDesc = _

  before {
    opDesc = new BarChartOpDesc()
  }

  it should "throw assertion error if value is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

  it should "list titles of axes in the python code" in {
    opDesc.fields = "geo.state_name"
    opDesc.value = "person.count"
    val temp = opDesc.manipulateTable()
    assert(temp.contains("geo.state_name"))
    assert(temp.contains("person.count"))
  }

  it should "throw assertion error if chart is empty" in {
    assertThrows[AssertionError] {
      opDesc.manipulateTable()
    }
  }

}
