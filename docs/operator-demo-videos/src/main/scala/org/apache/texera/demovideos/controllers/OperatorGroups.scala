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

package org.apache.texera.demovideos.controllers

import org.apache.texera.amber.operator.metadata.{GroupInfo, OperatorGroupConstants}

/** Flattens the operator group tree into group-name -> full path
  * (e.g. "Basic" -> Seq("Visualization", "Basic")).
  */
private[demovideos] object OperatorGroups {
  lazy val pathByName: Map[String, Seq[String]] =
    walk(OperatorGroupConstants.OperatorGroupOrderList, Seq.empty)

  private def walk(items: List[GroupInfo], prefix: Seq[String]): Map[String, Seq[String]] = {
    items.flatMap { g =>
      val path = prefix :+ g.groupName
      val current = Map(g.groupName -> path)
      val children = Option(g.children).getOrElse(List.empty)
      current ++ walk(children, path)
    }.toMap
  }
}
