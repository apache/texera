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

package org.apache.texera.amber.operator.substringSearch

import org.apache.texera.amber.core.tuple.Tuple
import org.apache.texera.amber.operator.filter.FilterOpExec
import org.apache.texera.amber.util.JSONUtils.objectMapper

class SubstringSearchOpExec(descString: String) extends FilterOpExec {
  private val desc: SubstringSearchOpDesc =
    objectMapper.readValue(descString, classOf[SubstringSearchOpDesc])

  this.setFilterFunc(findSubstring)

  private def findSubstring(tuple: Tuple): Boolean = {
    val field = tuple.getField[Any](desc.attribute)
    // A row with nothing in the column matches nothing. FilterPredicate answers the
    // same way: once a field is null, every condition but IS_NULL / IS_NOT_NULL is
    // false. An empty cell is ordinary input, since a blank in a CSV arrives as null.
    if (field == null) {
      false
    } else {
      val content = field.toString
      if (desc.isCaseSensitive) {
        content.contains(desc.substring)
      } else {
        content.toLowerCase.contains(desc.substring.toLowerCase)
      }
    }
  }
}
