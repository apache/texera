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

package org.apache.texera.demovideos.scripts

import org.apache.texera.demovideos.controllers.ControllerContext

/** `prepare` (create + import, unrecorded) runs before recording starts; `execute` is the
  * recorded demo. Login is the runner's job — once per run, session shared across scenarios.
  */
trait OperatorScript {
  def operatorName: String
  def operatorType: String = operatorName.replaceAll("\\s+", "")
  def category: String
  def outputFileName: String

  def prepare(ctx: ControllerContext): Unit = ()
  def execute(ctx: ControllerContext): Unit
}
