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

package org.apache.texera.amber.operator.visualization.wordCloud

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class WordCloudOpDescSpec extends AnyFlatSpec with BeforeAndAfter {

  var opDesc: WordCloudOpDesc = _

  before {
    opDesc = new WordCloudOpDesc()
  }

  it should "use correct regex pattern to match word characters" in {
    opDesc.textColumn = "text_col"
    val code = opDesc.manipulateTable().plain
    assert(
      code.contains("""r'\w'"""),
      "regex should use single backslash \\w to match word characters"
    )
    assert(
      !code.contains("""r'\\w'"""),
      "regex should not use double backslash \\\\w which matches literal backslash+w"
    )
  }

  it should "include the text column in manipulateTable" in {
    opDesc.textColumn = "my_text"
    val code = opDesc.manipulateTable().plain
    assert(code.contains("my_text"))
  }

  it should "include the text column in createWordCloudFigure" in {
    opDesc.textColumn = "my_text"
    val code = opDesc.createWordCloudFigure().plain
    assert(code.contains("my_text"))
  }
}
