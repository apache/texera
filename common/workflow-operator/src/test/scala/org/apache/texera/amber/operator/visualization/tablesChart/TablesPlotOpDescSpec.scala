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

package org.apache.texera.amber.operator.visualization.tablesChart

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TablesPlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: TablesPlotOpDesc = _

  before {
    opDesc = new TablesPlotOpDesc()
  }

  private def column(name: String): TablesConfig = {
    val config = new TablesConfig()
    config.attributeName = name
    config
  }

  it should "define the render_error the empty-table branches call" in {
    // Both empty-table branches call self.render_error; without the definition they
    // raised AttributeError instead of rendering the message.
    opDesc.includedColumns = List(column("col_one"), column("col_two"))
    val code = opDesc.generatePythonCode()
    code should include("def render_error(self, error_msg) -> str:")
    code should include(
      """return f"<h1>Tables Plot is not available.</h1><p>Reason is: {error_msg}</p>""""
    )
  }
}
