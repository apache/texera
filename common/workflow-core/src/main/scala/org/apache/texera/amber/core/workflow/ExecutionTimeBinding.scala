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

package org.apache.texera.amber.core.workflow

import org.apache.texera.amber.core.executor.OpExecInitInfo

/**
  * Setup an operator leaves until its execution starts.
  *
  * Compilation runs again on every edit of a workflow, so anything expensive there is paid far
  * more often than the workflow is run; and some answers — a directory inside the computing unit
  * that will run the operator — only exist at run time. Implementations must be idempotent: the
  * engine may ask more than once, and a region that re-executes asks again.
  */
trait ExecutionTimeBinding {

  /** Executor initialization info with every late-bound value in place. */
  def opExecInitInfo: OpExecInitInfo

  /** Repositories, as "<repositoryName>:<commitHash>", to mount before the operator runs. */
  def mountLocators: Set[String]
}
