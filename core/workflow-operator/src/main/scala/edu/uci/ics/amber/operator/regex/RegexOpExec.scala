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

package edu.uci.ics.amber.operator.regex

import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.operator.filter.FilterOpExec
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import java.util.regex.Pattern

class RegexOpExec(descString: String) extends FilterOpExec {
  private val desc: RegexOpDesc = objectMapper.readValue(descString, classOf[RegexOpDesc])
  lazy val pattern: Pattern =
    Pattern.compile(desc.regex, if (desc.caseInsensitive) Pattern.CASE_INSENSITIVE else 0)
  this.setFilterFunc(this.matchRegex)

  private def matchRegex(tuple: Tuple): Boolean =
    Option[Any](tuple.getField(desc.attribute).toString)
      .map(_.toString)
      .exists(value => pattern.matcher(value).find)
}
