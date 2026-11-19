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

package org.apache.texera.web.resource.dashboard.user.workflow

import javax.ws.rs.BadRequestException
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, Workflow}
import org.scalatest.flatspec.AnyFlatSpec

class WorkflowGraphIDsResourceSpec extends AnyFlatSpec {
  for (save <- List(false, true)) {
    "Workflow writes" should s"reject malformed JSON before ${if (save) "save" else "create"}" in {
      for (content <- List(null, "null", "not-json")) {
        val workflow = new Workflow()
        workflow.setContent(content)
        val resource = new WorkflowResource()
        val session = new SessionUser(new User())
        intercept[BadRequestException] {
          if (save) resource.persistWorkflow(workflow, session)
          else resource.createWorkflow(workflow, session)
        }
        assert(workflow.getWid == null)
      }
    }
    "Workflow writes" should s"reject duplicate IDs before any ${if (save) "save" else "create"} writes" in {
      val workflow = new Workflow()
      workflow.setContent("""{"operators":[{"operatorID":"same"}],"links":[{"linkID":"same"}]}""")
      val resource = new WorkflowResource()
      val session = new SessionUser(new User())
      val error = intercept[BadRequestException] {
        if (save) resource.persistWorkflow(workflow, session)
        else resource.createWorkflow(workflow, session)
      }
      assert(error.getMessage.contains("Duplicate graph element ID 'same'"))
      assert(workflow.getWid == null)
    }
  }
}
