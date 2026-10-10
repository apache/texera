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

package org.apache.texera.amber.operator.visualization.urlviz

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.util.JSONUtils.objectMapper

/**
  * URL Visualization operator to render any given URL link
  */
class UrlVizOpExec(descString: String) extends OperatorExecutor {
  private val desc: UrlVizOpDesc = objectMapper.readValue(descString, classOf[UrlVizOpDesc])
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    val url = UrlVizOpExec.escapeAttribute(
      String.valueOf(tuple.getField[Any](desc.urlContentAttrName))
    )
    val iframe =
      s"""<!DOCTYPE html>
         |<html lang="en">
         |<body>
         |  <div class="modal-body">
         |    <iframe src="$url" frameborder="0"
         |       style="height:100vh; width:100%; border:none;">
         |    </iframe>
         |  </div>
         |</body>
         |</html>""".stripMargin
    Iterator(TupleLike(iframe))
  }
}

object UrlVizOpExec {

  /** The value written where an attribute is expected.
    *
    * A cell is a value the workflow carried, not markup: a quote in it used to
    * close `src=` and leave the rest of the cell standing as attributes of the
    * iframe, which is the page speaking for whoever wrote the row. The five
    * characters and their order are Python's `html.escape(quote=True)`, which
    * the exported script calls on the same cell, so both paths write the page
    * the same way.
    */
  def escapeAttribute(value: String): String =
    value
      .replace("&", "&amp;")
      .replace("<", "&lt;")
      .replace(">", "&gt;")
      .replace("\"", "&quot;")
      .replace("'", "&#x27;")
}
