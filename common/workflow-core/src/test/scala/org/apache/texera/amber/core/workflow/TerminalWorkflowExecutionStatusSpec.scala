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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TerminalWorkflowExecutionStatusSpec extends AnyFlatSpec with Matchers {

  "TerminalWorkflowExecutionStatus.dbStatuses" should "be the codes amber writes for an ended execution" in {
    // Utils.maptoStatusCode: COMPLETED -> 3, FAILED -> 4, KILLED -> 5.
    TerminalWorkflowExecutionStatus.dbStatuses.map(_.intValue) shouldBe Seq(3, 4, 5)
  }
}
