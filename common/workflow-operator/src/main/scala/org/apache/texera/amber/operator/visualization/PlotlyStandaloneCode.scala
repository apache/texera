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

package org.apache.texera.amber.operator.visualization

import org.apache.texera.amber.operator.StandaloneCodeGenerator

/**
  * A generator whose emitted code draws with plotly.
  *
  * Mixed in rather than stated per operator because the three modules are one
  * dependency: a chart reaching for `px` today and `go` tomorrow would otherwise
  * have to remember to edit a list that nothing checks. What the mixin does say,
  * and the reason it is not simply always emitted, is that an operator NOT
  * mixing it in draws nothing, so a script built only from those runs wherever
  * pandas is installed.
  */
trait PlotlyStandaloneCode extends StandaloneCodeGenerator {

  override def standaloneImports(): Seq[String] =
    Seq(
      "import plotly.express as px",
      "import plotly.graph_objects as go",
      "import plotly.io"
    )
}
