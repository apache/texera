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

package org.apache.texera.amber.operator.visualization.figureFactoryTable

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FigureFactoryTableOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: FigureFactoryTableOpDesc = _

  before {
    opDesc = new FigureFactoryTableOpDesc()
  }

  private def column(name: String): FigureFactoryTableConfig = {
    val config = new FigureFactoryTableConfig()
    config.attributeName = name
    config
  }

  private def withColumns(): Unit =
    opDesc.columns = List(column("col_one"), column("col_two"))

  it should "define the render_error the empty-table branches call" in {
    // Both empty-table branches call self.render_error; without the definition they
    // raised AttributeError instead of rendering the message.
    withColumns()
    val code = opDesc.generatePythonCode()
    code should include("def render_error(self, error_msg) -> str:")
    code should include(
      """return f"<h1>Figure Factory Table is not available.</h1><p>Reason is: {error_msg}</p>""""
    )
  }
}
