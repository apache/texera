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

package edu.uci.ics.amber.operator.visualization.htmlviz

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

/**
  * HTML Visualization operator to render any given HTML code
  */
class HtmlVizOpExec(descString: String) extends OperatorExecutor {
  private val desc: HtmlVizOpDesc = objectMapper.readValue(descString, classOf[HtmlVizOpDesc])
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] =
    Iterator(TupleLike(tuple.getField[Any](desc.htmlContentAttrName)))
}
