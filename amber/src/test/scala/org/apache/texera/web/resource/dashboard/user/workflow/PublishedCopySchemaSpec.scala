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

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.tables.daos.WorkflowDao
import org.apache.texera.dao.jooq.generated.tables.pojos.Workflow
import org.jooq.exception.DataAccessException
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * The columns a pinned public copy lives in, and the constraint that keeps them honest.
  *
  * Nothing writes them yet: this covers what the migration alone guarantees -- that a workflow
  * public today keeps behaving as it does, and that a private workflow can never carry a pin.
  */
class PublishedCopySchemaSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private var workflowDao: WorkflowDao = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    workflowDao = new WorkflowDao(getDSLContext.configuration())
  }

  /** A workflow as it exists before anything pins it: content only, nothing frozen. */
  private def insertWorkflow(name: String, isPublic: Boolean): Workflow = {
    val workflow = new Workflow()
    workflow.setName(name)
    workflow.setDescription("a workflow")
    workflow.setContent("""{"operators":[]}""")
    workflow.setIsPublic(isPublic)
    workflowDao.insert(workflow)
    workflowDao.fetchOneByWid(workflow.getWid)
  }

  behavior of "the published-copy columns"

  it should "leave every workflow following the author's latest" in {
    // The migration adds columns and no backfill, so a workflow that was public before it ran shows
    // exactly what it showed: nothing is frozen, which is the state the rest of the feature calls
    // "following".
    val stored = insertWorkflow("migration_changes_nothing", isPublic = true)

    stored.getPublishedContent shouldBe null
    stored.getPublishedName shouldBe null
    stored.getPublishedDescription shouldBe null
    stored.getPublishedVersionId shouldBe null
  }

  it should "let a public workflow carry a frozen copy" in {
    val stored = insertWorkflow("public_may_be_pinned", isPublic = true)
    stored.setPublishedContent("""{"operators":[]}""")
    stored.setPublishedName("frozen name")
    stored.setPublishedDescription("frozen description")

    workflowDao.update(stored)

    workflowDao.fetchOneByWid(stored.getWid).getPublishedName shouldBe "frozen name"
  }

  it should "refuse a private workflow that carries a frozen copy" in {
    // A pin only means something while the workflow is public. The database rejects the other case,
    // so no code path can leave one behind -- unpublishing has to clear the copy.
    val stored = insertWorkflow("private_cannot_be_pinned", isPublic = false)
    stored.setPublishedContent("""{"operators":[]}""")

    a[DataAccessException] should be thrownBy workflowDao.update(stored)
  }
}
