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

package org.apache.texera.amber.operator.udf.python

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatasetVariableMappingSpec extends AnyFlatSpec with Matchers {

  "DatasetVariableMapping" should "default to empty variable and dataset" in {
    val mapping = new DatasetVariableMapping()
    mapping.variableName shouldBe ""
    mapping.datasetPath shouldBe ""
  }

  it should "hold the variable name and dataset path assigned to it" in {
    val mapping = new DatasetVariableMapping()
    mapping.variableName = "A"
    mapping.datasetPath = "/bob@texera.com/twitterDataset/v1"
    mapping.variableName shouldBe "A"
    mapping.datasetPath shouldBe "/bob@texera.com/twitterDataset/v1"
  }
}
