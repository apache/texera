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

package org.apache.texera.web.resource.dashboard.file

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{
  USER,
  WORKFLOW,
  WORKFLOW_EXECUTIONS,
  WORKFLOW_USER_CLONES,
  WORKFLOW_VERSION
}
import org.apache.texera.dao.jooq.generated.enums.{DefaultViewEnum, PrivilegeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowUserAccessDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, Workflow, WorkflowUserAccess}
import org.apache.texera.web.resource.dashboard.DashboardResource.SearchQueryParams
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowResource.CoverImageRequest
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowResource
import org.apache.texera.web.resource.dashboard.user.workflow.WorkflowResource.{
  DashboardWorkflow,
  DefaultViewRequest,
  WorkflowIDs,
  WorkflowWithPrivilege
}
import org.apache.texera.web.resource.dashboard.{DashboardResource, FulltextSearchQueryUtils}
import org.jooq.Condition
import org.jooq.impl.DSL.noCondition
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}
import java.time.{Duration, OffsetDateTime, ZoneOffset}
import java.util
import java.util.Collections
import java.util.concurrent.TimeUnit
import javax.servlet.http.HttpServletRequest
import javax.ws.rs.{
  BadRequestException,
  ForbiddenException,
  NotFoundException,
  WebApplicationException
}
import scala.jdk.CollectionConverters._
import org.apache.texera.web.resource.dashboard.user.workflow.{
  WorkflowAccessResource,
  WorkflowVersionResource
}

class WorkflowResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockFactory
    with MockTexeraDB {

  // An example creation time to test Account Creation Time attribute
  private val exampleCreationTime: OffsetDateTime =
    OffsetDateTime.parse("2025-01-01T00:00:00Z")

  private val testUser: User = {
    val user = new User
    user.setUid(Integer.valueOf(1))
    user.setName("test_user")
    user.setEmail("test_user@mail.com")
    user.setRole(UserRoleEnum.ADMIN)
    user.setComment("test_comment")
    user.setAccountCreationTime(exampleCreationTime)
    user
  }

  private val testUser2: User = {
    val user = new User
    user.setUid(Integer.valueOf(2))
    user.setName("test_user2")
    user.setEmail("test_user2@mail.com")
    user.setRole(UserRoleEnum.ADMIN)
    user.setComment("test_comment2")
    user.setAccountCreationTime(exampleCreationTime)
    user
  }

  private val keywordInWorkflow1Content = "keyword_in_workflow1_content"
  private val textPhrase = "text phrases"
  private val exampleContent =
    "{\"x\":5,\"y\":\"" + keywordInWorkflow1Content + "\",\"z\":\"" + textPhrase + "\"}"

  private val testWorkflow1: Workflow = {
    val workflow = new Workflow()
    workflow.setName("test_workflow1")
    workflow.setDescription("keyword_in_workflow_description")
    workflow.setContent(exampleContent)

    workflow
  }

  private val testWorkflow2: Workflow = {
    val workflow = new Workflow()
    workflow.setName("test_workflow2")
    workflow.setDescription("another_text")
    workflow.setContent("{\"x\":5,\"y\":\"example2\",\"z\":\"\"}")

    workflow
  }

  private val testWorkflow3: Workflow = {
    val workflow = new Workflow()
    workflow.setName("test_workflow3")
    workflow.setDescription("")
    workflow.setContent("{\"x\":5,\"y\":\"example3\",\"z\":\"\"}")

    workflow
  }

  private val exampleEmailAddress = "name@example.com"
  private val exampleWord1 = "Lorem"
  private val exampleWord2 = "Ipsum"

  private val testWorkflowWithSpecialCharacters: Workflow = {
    val workflow = new Workflow()
    workflow.setName("workflow_with_special_characters")
    workflow.setDescription(exampleWord1 + " " + exampleWord2 + " " + exampleEmailAddress)
    workflow.setContent(exampleContent)

    workflow
  }

  private val sessionUser1: SessionUser = {
    new SessionUser(testUser)
  }

  private val sessionUser2: SessionUser = {
    new SessionUser(testUser2)
  }

  private val workflowResource: WorkflowResource = {
    new WorkflowResource()
  }

  private val dashboardResource: DashboardResource = {
    new DashboardResource()
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    FulltextSearchQueryUtils.usePgroonga = false // disable pgroonga
    // add test user directly
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(testUser)
    userDao.insert(testUser2)
  }

  override protected def beforeEach(): Unit = {
    // Clean up environment before each test case
    // Delete all workflows, or reset the state of the `workflowResource` object
  }

  override protected def afterEach(): Unit = {
    // Clean up environment after each test case if necessary
    // delete all workflows in the database
    var workflows = workflowResource.retrieveWorkflowsBySessionUser(sessionUser1)
    workflows.foreach(workflow =>
      workflowResource.deleteWorkflow(
        WorkflowIDs(List(workflow.workflow.getWid)),
        sessionUser1
      )
    )

    workflows = workflowResource.retrieveWorkflowsBySessionUser(sessionUser2)
    workflows.foreach(workflow =>
      workflowResource.deleteWorkflow(
        WorkflowIDs(List(workflow.workflow.getWid)),
        sessionUser2
      )
    )
  }

  override protected def afterAll(): Unit = {
    closeConnectionPool()
  }

  private def getKeywordsArray(keywords: String*): util.ArrayList[String] = {
    val keywordsList = new util.ArrayList[String]()
    for (keyword <- keywords) {
      keywordsList.add(keyword)
    }
    keywordsList
  }

  private def insertAndAssertAccountCreation(uid: Int, ts: OffsetDateTime): Unit = {
    val userDao = new UserDao(getDSLContext.configuration())
    val u = new User
    u.setUid(Integer.valueOf(uid))
    u.setName(s"tmp_user_$uid")
    u.setRole(UserRoleEnum.REGULAR)
    u.setComment("tmp")
    u.setAccountCreationTime(ts)
    userDao.insert(u)

    try {
      val fetched = userDao.fetchOneByUid(Integer.valueOf(uid))
      assert(fetched.getAccountCreationTime != null)
      assert(fetched.getAccountCreationTime.isEqual(ts))
    } finally {
      userDao.deleteById(Integer.valueOf(uid))
    }
  }

  private def assertSameWorkflow(a: Workflow, b: DashboardWorkflow): Unit = {
    assert(a.getName == b.workflow.getName)
  }

  "User.accountCreationTime" should "be persisted and retrievable via UserDao" in {
    val userDao = new UserDao(getDSLContext.configuration())
    val u1 = userDao.fetchOneByUid(Integer.valueOf(1))
    val u2 = userDao.fetchOneByUid(Integer.valueOf(2))

    assert(u1.getAccountCreationTime != null)
    assert(u2.getAccountCreationTime != null)

    assert(u1.getAccountCreationTime.isEqual(exampleCreationTime))
    assert(u2.getAccountCreationTime.isEqual(exampleCreationTime))
  }

  it should "remain unchanged when updating unrelated fields" in {
    val userDao = new UserDao(getDSLContext.configuration())
    val u1 = userDao.fetchOneByUid(Integer.valueOf(1))
    val originalTime = u1.getAccountCreationTime

    u1.setComment("updated_comment")
    userDao.update(u1)

    val test_u1 = userDao.fetchOneByUid(Integer.valueOf(1))
    assert(test_u1.getAccountCreationTime.isEqual(originalTime))
  }

  it should "fallback to DB default when not explicitly set on insert" in {
    // account_creation_time TIMESTAMPTZ NOT NULL DEFAULT now()
    val userDao = new UserDao(getDSLContext.configuration())
    // Test user 3 on top of test user 1 and 2
    val userId = 3
    val tmp = new User
    tmp.setUid(Integer.valueOf(userId))
    tmp.setName("tmp_user")
    tmp.setRole(UserRoleEnum.REGULAR)
    tmp.setComment("tmp")
    // Account creation time not set
    userDao.insert(tmp)

    val fetched = userDao.fetchOneByUid(Integer.valueOf(3))
    assert(fetched.getAccountCreationTime != null)

    val now = OffsetDateTime.now(ZoneOffset.UTC)
    val diff = Duration.between(fetched.getAccountCreationTime, now).abs()
    assert(diff.toMinutes <= 2)
  }

  // Testing with user id 4
  it should "persist and retrieve a non-UTC offset time (ex: +09:00 JST)" in {
    val userId = 4
    insertAndAssertAccountCreation(
      uid = userId,
      ts = OffsetDateTime.parse("2020-06-15T12:34:56+09:00")
    )
  }

  // Testing with user id 5
  it should "persist and retrieve a leap day timestamp" in {
    val userId = 5
    insertAndAssertAccountCreation(
      uid = userId,
      ts = OffsetDateTime.parse("2024-02-29T23:59:59Z")
    )
  }

  // Testing with user id 6
  it should "persist and retrieve a future timestamp" in {
    val userId = 6
    insertAndAssertAccountCreation(
      uid = userId,
      ts = OffsetDateTime.parse("2100-12-31T23:59:59Z")
    )
  }

  "WorkflowResource /owner_name" should "return owner name as plain text" in {
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)

    val workflows = workflowResource.retrieveWorkflowsBySessionUser(sessionUser1)
    assert(workflows.nonEmpty)

    val wid =
      workflows
        .find(_.workflow.getName == testWorkflow1.getName)
        .map(_.workflow.getWid)
        .getOrElse(workflows.head.workflow.getWid)

    val ownerName = workflowResource.getOwnerName(wid)

    assert(ownerName == testUser.getName)
  }

  "/search API" should "be able to search for workflows in different columns in Workflow table" in {
    // testWorkflow1: {name: test_name, descrption: test_description, content: test_content}
    // search "test_name" or "test_description" or "test_content" should return testWorkflow1
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    // search
    val DashboardWorkflowEntryList =
      dashboardResource
        .searchAllResourcesCall(
          sessionUser1,
          SearchQueryParams(keywords = getKeywordsArray(keywordInWorkflow1Content))
        )
        .results
    assert(DashboardWorkflowEntryList.head.workflow.get.ownerName.equals(testUser.getName))
    assert(DashboardWorkflowEntryList.length == 1)
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList.head.workflow.get)
  }

  it should "be able to search text phrases" in {
    // testWorkflow1: {name: "test_name", descrption: "test_description", content: "text phrase"}
    // search "text phrase" should return testWorkflow1
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    val DashboardWorkflowEntryList =
      dashboardResource
        .searchAllResourcesCall(
          sessionUser1,
          SearchQueryParams(keywords = getKeywordsArray(keywordInWorkflow1Content))
        )
        .results
    assert(DashboardWorkflowEntryList.length == 1)
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList.head.workflow.get)
    val DashboardWorkflowEntryList1 =
      dashboardResource
        .searchAllResourcesCall(
          sessionUser1,
          SearchQueryParams(keywords = getKeywordsArray("text sear"))
        )
        .results
    assert(DashboardWorkflowEntryList1.isEmpty)
  }

  it should "return an all workflows when given an empty list of keywords" in {
    // search "" should return all workflows
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    val DashboardWorkflowEntryList =
      dashboardResource.searchAllResourcesCall(sessionUser1, SearchQueryParams())
    assert(DashboardWorkflowEntryList.results.length == 2)
  }

  it should "return only single instance of workflow when owned and shared publicly" in {
    // Create a public workflow
    val publicWorkflow = new Workflow()
    publicWorkflow.setName("public_workflow_1")
    publicWorkflow.setDescription(testWorkflow1.getDescription)
    publicWorkflow.setContent(testWorkflow1.getContent)
    publicWorkflow.setIsPublic(true)

    // Persist workflow with testUser as owner
    workflowResource.persistWorkflow(publicWorkflow, sessionUser1)

    val DashboardWorkflowEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(),
        includePublic = true
      )
    assert(DashboardWorkflowEntryList.results.length == 1)
    assertSameWorkflow(publicWorkflow, DashboardWorkflowEntryList.results.head.workflow.get)
  }

  it should "return only single instance of workflow when publicly and explicitly shared" in {
    // Create a public workflow
    val publicWorkflow = new Workflow()
    publicWorkflow.setName("public_workflow_2")
    publicWorkflow.setDescription(testWorkflow1.getDescription)
    publicWorkflow.setContent(testWorkflow1.getContent)
    publicWorkflow.setIsPublic(true)

    // Persist workflow with testUser as owner
    val savedWorkflow = workflowResource.persistWorkflow(publicWorkflow, sessionUser1)

    // Share workflow with read access to testUser2
    val workflowAccessResource = new WorkflowAccessResource()
    workflowAccessResource.grantAccess(
      savedWorkflow.getWid,
      testUser2.getEmail,
      "READ",
      sessionUser1
    )

    val DashboardWorkflowEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser2,
        SearchQueryParams(),
        includePublic = true
      )
    assert(DashboardWorkflowEntryList.results.length == 1)
    assertSameWorkflow(publicWorkflow, DashboardWorkflowEntryList.results.head.workflow.get)
  }

  it should "be able to search with arbitrary number of keywords in different combinations" in {
    // testWorkflow1: {name: test_name, description: test_description, content: "key pair"}
    // search ["key"] or ["pair", "key"] should return the testWorkflow1
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    // search with multiple keywords
    val keywords = new util.ArrayList[String]()
    keywords.add(keywordInWorkflow1Content)
    keywords.add(testWorkflow1.getDescription)
    val DashboardWorkflowEntryList = dashboardResource
      .searchAllResourcesCall(sessionUser1, SearchQueryParams(keywords = keywords))
      .results
    assert(DashboardWorkflowEntryList.size == 1)
    assert(DashboardWorkflowEntryList.head.workflow.get.ownerName.equals(testUser.getName))
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList.head.workflow.get)

    keywords.add("nonexistent")
    val DashboardWorkflowEntryList2 = dashboardResource
      .searchAllResourcesCall(sessionUser1, SearchQueryParams(keywords = keywords))
      .results
    assert(DashboardWorkflowEntryList2.isEmpty)

    val keywordsReverseOrder = new util.ArrayList[String]()
    keywordsReverseOrder.add(testWorkflow1.getDescription)
    keywordsReverseOrder.add(keywordInWorkflow1Content)
    val DashboardWorkflowEntryList1 =
      dashboardResource
        .searchAllResourcesCall(sessionUser1, SearchQueryParams(keywords = keywordsReverseOrder))
        .results
    assert(DashboardWorkflowEntryList1.size == 1)
    assert(DashboardWorkflowEntryList1.head.workflow.get.ownerName.equals(testUser.getName))
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList1.head.workflow.get)

  }

  it should "handle reserved characters in the keywords" in {
    // testWorkflow1: {name: test_name, description: test_description, content: "key pair"}
    // search "key+-pair" or "key@pair" or "key+" or "+key" should return testWorkflow1
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)

    def testInner(keywords: String): Unit = {
      val DashboardWorkflowEntryList = dashboardResource
        .searchAllResourcesCall(
          sessionUser1,
          SearchQueryParams(keywords = getKeywordsArray(keywords))
        )
        .results
      assert(DashboardWorkflowEntryList.size == 1)
      assert(DashboardWorkflowEntryList.head.workflow.get.ownerName.equals(testUser.getName))
      assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList.head.workflow.get)
    }

    testInner(keywordInWorkflow1Content + "+-@()<>~*\"" + keywordInWorkflow1Content)
    testInner(keywordInWorkflow1Content + "@" + keywordInWorkflow1Content)
    testInner(keywordInWorkflow1Content + "+-@()<>~*\"")
    testInner("+-@()<>~*\"" + keywordInWorkflow1Content)

  }

  it should "return all workflows when keywords only contains reserved keywords +-@()<>~*\"" in {
    // search "+-@()<>~*"" should return all workflows
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)

    val DashboardWorkflowEntryList =
      dashboardResource
        .searchAllResourcesCall(sessionUser1, SearchQueryParams(getKeywordsArray("+-@()<>~*\"")))
        .results
    assert(DashboardWorkflowEntryList.size == 2)

  }

  it should "not be able to search workflows from different user accounts" in {
    // user1 has workflow1
    // user2 has workflow2
    // users should only be able to search for workflows they have access to
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow2, sessionUser2)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)

    def test(user: SessionUser, workflow: Workflow): Unit = {
      // search with reserved characters in keywords
      val DashboardWorkflowEntryList =
        dashboardResource
          .searchAllResourcesCall(
            user,
            SearchQueryParams(getKeywordsArray(workflow.getDescription))
          )
          .results
      assert(DashboardWorkflowEntryList.size == 1)
      assert(DashboardWorkflowEntryList.head.workflow.get.ownerName.equals(user.getName()))
      assertSameWorkflow(workflow, DashboardWorkflowEntryList.head.workflow.get)
    }

    test(sessionUser1, testWorkflow1)
    test(sessionUser2, testWorkflow2)
  }

  it should "return a proper condition for a single owner" in {
    val ownerList = new java.util.ArrayList[String](util.Arrays.asList("owner1"))
    val ownerFilter: Condition =
      FulltextSearchQueryUtils.getContainsFilter(ownerList, USER.EMAIL)
    assert(ownerFilter.toString == USER.EMAIL.eq("owner1").toString)
  }

  it should "return a proper condition for multiple owners" in {
    val ownerList = new java.util.ArrayList[String](util.Arrays.asList("owner1", "owner2"))
    val ownerFilter: Condition =
      FulltextSearchQueryUtils.getContainsFilter(ownerList, USER.EMAIL)
    assert(ownerFilter.toString == USER.EMAIL.eq("owner1").or(USER.EMAIL.eq("owner2")).toString)
  }

  it should "return a proper condition for a single workflowID" in {
    val workflowIdList = new java.util.ArrayList[Integer](util.Arrays.asList(Integer.valueOf(1)))
    val workflowIdFilter: Condition =
      FulltextSearchQueryUtils.getContainsFilter(workflowIdList, WORKFLOW.WID)
    assert(workflowIdFilter.toString == WORKFLOW.WID.eq(Integer.valueOf(1)).toString)
  }

  it should "return a proper condition for multiple workflowIDs" in {
    val workflowIdList = new java.util.ArrayList[Integer](
      util.Arrays.asList(Integer.valueOf(1), Integer.valueOf(2))
    )
    val workflowIdFilter: Condition =
      FulltextSearchQueryUtils.getContainsFilter(workflowIdList, WORKFLOW.WID)
    assert(
      workflowIdFilter.toString == WORKFLOW.WID
        .eq(Integer.valueOf(1))
        .or(WORKFLOW.WID.eq(Integer.valueOf(2)))
        .toString
    )
  }

  it should "return a proper condition for creation date type with specific start and end date" in {
    val dateFilter: Condition =
      FulltextSearchQueryUtils.getDateFilter(
        "2023-01-01",
        "2023-12-31",
        WORKFLOW.CREATION_TIME
      )
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startTimestamp = new Timestamp(dateFormat.parse("2023-01-01").getTime)
    val endTimestamp =
      new Timestamp(
        dateFormat.parse("2023-12-31").getTime + TimeUnit.DAYS.toMillis(1) - 1
      )
    assert(
      dateFilter.toString == WORKFLOW.CREATION_TIME.between(startTimestamp, endTimestamp).toString
    )
  }

  it should "return a proper condition for modification date type with specific start and end date" in {
    val dateFilter: Condition =
      FulltextSearchQueryUtils.getDateFilter(
        "2023-01-01",
        "2023-12-31",
        WORKFLOW.LAST_MODIFIED_TIME
      )
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startTimestamp = new Timestamp(dateFormat.parse("2023-01-01").getTime)
    val endTimestamp =
      new Timestamp(
        dateFormat.parse("2023-12-31").getTime + TimeUnit.DAYS.toMillis(1) - 1
      )
    assert(
      dateFilter.toString == WORKFLOW.LAST_MODIFIED_TIME
        .between(startTimestamp, endTimestamp)
        .toString
    )
  }

  it should "throw a ParseException when endDate is invalid" in {
    assertThrows[ParseException] {
      FulltextSearchQueryUtils.getDateFilter(
        "2023-01-01",
        "invalidDate",
        WORKFLOW.CREATION_TIME
      )
    }
  }

  "getOperatorsFilter" should "return a noCondition when the input operators list is empty" in {
    val operatorsFilter: Condition =
      FulltextSearchQueryUtils.getOperatorsFilter(
        Collections.emptyList[String](),
        WORKFLOW.CONTENT
      )
    assert(operatorsFilter.toString == noCondition().toString)
  }

  it should "return a proper condition for a single operator" in {
    val operatorsList = new java.util.ArrayList[String](util.Arrays.asList("operator1"))
    val operatorsFilter: Condition =
      FulltextSearchQueryUtils.getOperatorsFilter(operatorsList, WORKFLOW.CONTENT)
    val searchKey = "%\"operatorType\":\"operator1\"%"
    assert(operatorsFilter.toString == WORKFLOW.CONTENT.likeIgnoreCase(searchKey).toString)
  }

  it should "return a proper condition for multiple operators" in {
    val operatorsList =
      new java.util.ArrayList[String](util.Arrays.asList("operator1", "operator2"))
    val operatorsFilter: Condition =
      FulltextSearchQueryUtils.getOperatorsFilter(operatorsList, WORKFLOW.CONTENT)
    val searchKey1 = "%\"operatorType\":\"operator1\"%"
    val searchKey2 = "%\"operatorType\":\"operator2\"%"
    assert(
      operatorsFilter.toString == WORKFLOW.CONTENT
        .likeIgnoreCase(searchKey1)
        .or(WORKFLOW.CONTENT.likeIgnoreCase(searchKey2))
        .toString
    )
  }

  "/search API" should "be able to search for resources by keyword" in {

    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    // search
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(getKeywordsArray("test"))
      )
    assert(DashboardClickableFileEntryList.results.length == 1)

  }

  it should "return all resources when no keyword provided" in {
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(getKeywordsArray(""))
      )
    assert(DashboardClickableFileEntryList.results.length == 1)
  }

  it should "return multiple matching resources from a single resource type" in {
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow2, sessionUser1)
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(getKeywordsArray("test"))
      )
    assert(DashboardClickableFileEntryList.results.length == 2)
  }

  it should "handle multiple keywords correctly" in {
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow2, sessionUser1)
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(getKeywordsArray("test", "workflow1"))
      )
    assert(
      DashboardClickableFileEntryList.results.length == 1
    ) // should only return test_workflow1
  }

  it should "filter results by different resourceType" in {
    // create 3 workflows
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow2, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    // search resources with all resourceType
    var DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(getKeywordsArray("test"))
      )
    assert(DashboardClickableFileEntryList.results.length == 3)

    // filter resources by workflow
    DashboardClickableFileEntryList = dashboardResource.searchAllResourcesCall(
      sessionUser1,
      SearchQueryParams(resourceType = "workflow", keywords = getKeywordsArray("test"))
    )
    assert(DashboardClickableFileEntryList.results.length == 3)

    // filter resources by dataset
    DashboardClickableFileEntryList = dashboardResource.searchAllResourcesCall(
      sessionUser1,
      SearchQueryParams(resourceType = "dataset", keywords = getKeywordsArray("test"))
    )
    assert(DashboardClickableFileEntryList.results.isEmpty)

    // The counts above cannot distinguish a working filter from an ignored one, because every
    // seeded row is a workflow and the only other searchable types are LakeFS-backed (a seeded
    // dataset is dropped during hydration when LakeFS is unreachable, so it cannot be counted
    // here -- DatasetSearchQueryBuilderSpec covers that path against a stub). Asserting that an
    // unrecognised value is rejected pins that resourceType is dispatched on, not ignored.
    assertThrows[IllegalArgumentException] {
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(resourceType = "project", keywords = getKeywordsArray("test"))
      )
    }
  }

  it should "return resources that match any of all provided keywords" in {
    // This test is designed to verify that the searchAllResources function correctly
    // returns resources that match all of the provided keywords

    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow2, sessionUser1)
    // Perform search with multiple keywords
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(keywords = getKeywordsArray("test", "workflow2"))
      )

    // Assert that the search results include resources that match any of the provided keywords
    assert(DashboardClickableFileEntryList.results.length == 1)
  }

  it should "not return resources that belong to a different user" in {
    // This test is designed to verify that the searchAllResources function does not return resources that belong to a different user

    // Create a workflow for a different user (sessionUser2)
    workflowResource.persistWorkflow(testWorkflow1, sessionUser2)

    // Perform search for resources using sessionUser1
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(keywords = getKeywordsArray("test"))
      )

    // Assert that the search results do not include the workflow that belongs to the different user
    // Assuming that DashboardClickableFileEntryList is a list of resources where each resource has a `user` property
    assert(DashboardClickableFileEntryList.results.isEmpty)
  }

  it should "paginate results correctly" in {
    // This test is designed to verify that the pagination works correctly

    // Create 11 workflows
    for (i <- 1 to 11) {
      val workflow = new Workflow()
      workflow.setName(s"test_pagination_workflow$i")
      workflow.setDescription("")
      workflow.setContent(exampleContent)
      workflowResource.persistWorkflow(workflow, sessionUser1)
    }

    // Request the first page of results (page size is 10)
    val firstPage =
      dashboardResource.searchAllResourcesCall(sessionUser1, SearchQueryParams(count = 10))

    // Assert that the first page has 10 results
    assert(firstPage.results.length == 10)
    assert(firstPage.more) // Assert that there are more results to be fetched

    // Request the second page of results
    val secondPage =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(count = 10, offset = 10)
      )

    // Assert that the second page has 1 results
    assert(secondPage.results.length == 1)

    // Assert that the results are unique across all pages
    val allResults = firstPage.results ++ secondPage.results
    assert(allResults.distinct.length == allResults.length)
  }

  it should "order workflow by name correctly" in {
    // Create several resources with different names
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow2, sessionUser1)

    // Retrieve resources ordered by name in ascending order
    var resources =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(resourceType = "workflow", orderBy = "NameAsc")
      )

    // Check the order of the results
    assert(resources.results(0).workflow.get.workflow.getName == "test_workflow1")
    assert(resources.results(1).workflow.get.workflow.getName == "test_workflow2")
    assert(resources.results(2).workflow.get.workflow.getName == "test_workflow3")

    resources = dashboardResource.searchAllResourcesCall(
      sessionUser1,
      SearchQueryParams(resourceType = "workflow", orderBy = "NameDesc")
    )
    // Check the order of the results
    assert(resources.results(0).workflow.get.workflow.getName == "test_workflow3")
    assert(resources.results(1).workflow.get.workflow.getName == "test_workflow2")
    assert(resources.results(2).workflow.get.workflow.getName == "test_workflow1")
  }

  it should "order workflow by execution time correctly" in {
    // Create several resources with different names (no execution times are set, but the SQL query should parse correctly)
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow2, sessionUser1)

    // Retrieve resources ordered by execution time ascending
    var resources =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(resourceType = "workflow", orderBy = "ExecutionTimeAsc")
      )

    // Execution times are null so order is not guaranteed, but we verify it returns 3 results
    assert(resources.results.length == 3)

    // Retrieve resources ordered by execution time descending
    resources = dashboardResource.searchAllResourcesCall(
      sessionUser1,
      SearchQueryParams(resourceType = "workflow", orderBy = "ExecutionTimeDesc")
    )

    // Verify it returns 3 results
    assert(resources.results.length == 3)
  }

  it should "include workflow cover image in search results" in {
    // Create workflow
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)

    // Set cover image
    val workflowId =
      workflowResource.retrieveWorkflowsBySessionUser(sessionUser1).head.workflow.getWid

    val coverImage = "data:image/jpeg;base64,/9j/4AAQSkZJRg=="
    workflowResource.setCoverImage(
      workflowId,
      CoverImageRequest(coverImage),
      sessionUser1
    )

    // Search workflows
    val results =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(resourceType = "workflow")
      )

    // Verify cover image is included in response
    assert(results.results.length == 1)

    val workflowEntry = results.results.head.workflow.get
    assert(workflowEntry.coverImage.contains(coverImage))
  }

  it should "create a workflow with coverImage set to None" in {
    val workflow = new Workflow()
    workflow.setName("test_create_workflow")
    workflow.setContent(exampleContent)

    val result = workflowResource.createWorkflow(workflow, sessionUser1)

    assert(result.workflow.getName == "test_create_workflow")
    assert(result.coverImage.isEmpty)
  }

  // ─── read/query and mutation endpoints (issue #7224) ────────────────────────

  // Each test seeds its own workflow (createWorkflow mutates the pojo's wid, so a
  // shared fixture cannot be re-created); afterEach deletes them.
  private def seedWorkflow(
      user: SessionUser,
      name: String,
      description: String = "desc",
      content: String = "{}"
  ): DashboardWorkflow = {
    val workflow = new Workflow()
    workflow.setName(name)
    workflow.setDescription(description)
    workflow.setContent(content)
    workflowResource.createWorkflow(workflow, user)
  }

  "WorkflowResource.retrieveWorkflow" should "return the workflow for a user with access" in {
    val wid = seedWorkflow(sessionUser1, "retrieve-me", "the-desc", "{\"a\":1}").workflow.getWid
    val result: WorkflowWithPrivilege = workflowResource.retrieveWorkflow(wid, sessionUser1)
    assert(result.wid == wid)
    assert(result.name == "retrieve-me")
    assert(result.description == "the-desc")
    assert(!result.readonly) // the owner has write access
  }

  it should "throw ForbiddenException for a user without access" in {
    val wid = seedWorkflow(sessionUser1, "no-access-wf").workflow.getWid
    assertThrows[ForbiddenException](workflowResource.retrieveWorkflow(wid, sessionUser2))
  }

  "WorkflowResource.retrieveIDs" should "return the ids of the user's accessible workflows" in {
    val wid = seedWorkflow(sessionUser1, "id-wf").workflow.getWid
    assert(workflowResource.retrieveIDs(sessionUser1).asScala.contains(wid.toString))
  }

  "WorkflowResource.retrieveOwners" should "return the owner email of every accessible workflow" in {
    assert(workflowResource.retrieveOwners(sessionUser2).isEmpty) // user2 has no access yet
    val wid = seedWorkflow(sessionUser1, "owned-wf").workflow.getWid

    // the owner sees itself as the owner
    assert(workflowResource.retrieveOwners(sessionUser1).asScala.toList == List(testUser.getEmail))

    // grant user2 read access -> user2 now sees user1 (the owner), not itself
    new WorkflowUserAccessDao(getDSLContext.configuration())
      .merge(new WorkflowUserAccess(testUser2.getUid, wid, PrivilegeEnum.READ))
    assert(workflowResource.retrieveOwners(sessionUser2).asScala.toList == List(testUser.getEmail))
  }

  "WorkflowResource name/type/description/owner/size lookups" should "reflect the seeded workflow" in {
    val content = "{\"nodes\":42}"
    val wid = seedWorkflow(sessionUser1, "lookup-wf", "look-desc", content).workflow.getWid

    assert(workflowResource.getWorkflowName(wid) == "lookup-wf")
    assert(workflowResource.getWorkflowType(wid) == "Private") // not public by default
    assert(workflowResource.getWorkflowDescription(wid) == "look-desc")
    assert(workflowResource.getOwnerName(wid) == testUser.getName)

    val sizes = workflowResource.getSize(java.util.List.of(wid))
    assert(sizes.get(wid) == content.length)
  }

  "WorkflowResource.getSize" should "return an empty map for a null or empty id list" in {
    assert(workflowResource.getSize(null).isEmpty)
    assert(workflowResource.getSize(Collections.emptyList()).isEmpty)
  }

  "WorkflowResource.updateWorkflowName" should "rename the workflow" in {
    val wid = seedWorkflow(sessionUser1, "before-name").workflow.getWid
    val update = new Workflow()
    update.setWid(wid)
    update.setName("after-name")

    workflowResource.updateWorkflowName(update, sessionUser1)
    assert(workflowResource.getWorkflowName(wid) == "after-name")
  }

  "WorkflowResource.updateWorkflowDescription" should "update the description" in {
    val wid = seedWorkflow(sessionUser1, "desc-wf", "old-desc").workflow.getWid
    val update = new Workflow()
    update.setWid(wid)
    update.setDescription("new-desc")

    workflowResource.updateWorkflowDescription(update, sessionUser1)
    assert(workflowResource.getWorkflowDescription(wid) == "new-desc")
  }

  "WorkflowResource.makePublic / makePrivate" should "flip the public flag and publish/unpublish the workflow" in {
    val wid = seedWorkflow(sessionUser1, "pub-wf").workflow.getWid
    assert(workflowResource.getWorkflowType(wid) == "Private")

    workflowResource.makePublic(wid, sessionUser1)
    assert(workflowResource.getWorkflowType(wid) == "Public")
    assert(workflowResource.retrievePublicWorkflow(wid).wid == wid)

    workflowResource.makePrivate(wid, sessionUser1)
    assert(workflowResource.getWorkflowType(wid) == "Private")
  }

  it should "reject a user without write access with ForbiddenException" in {
    val wid = seedWorkflow(sessionUser1, "pub-forbidden").workflow.getWid
    assertThrows[ForbiddenException](workflowResource.makePublic(wid, sessionUser2))
  }

  "WorkflowResource.searchWorkflowByOperator" should "return only workflows whose content contains the operator" in {
    val wid = seedWorkflow(
      sessionUser1,
      "csv-wf",
      "d",
      "{\"operators\":[{\"operatorType\":\"CSVFileScan\"}]}"
    ).workflow.getWid
    seedWorkflow(sessionUser1, "filter-wf", "d", "{\"operators\":[{\"operatorType\":\"Filter\"}]}")

    val hits = workflowResource.searchWorkflowByOperator("CSVFileScan", sessionUser1)
    assert(hits == List(wid.toString))
  }

  "WorkflowResource.duplicateWorkflow" should "create a distinct copy owned by the user" in {
    // duplicateWorkflow reassigns operator ids, so the content must have an operators array.
    val wid = seedWorkflow(
      sessionUser1,
      "dup-src",
      "d",
      "{\"operators\":[{\"operatorID\":\"op1\",\"operatorType\":\"CSVFileScan\"}]}"
    ).workflow.getWid

    val copies = workflowResource.duplicateWorkflow(WorkflowIDs(List(wid)), sessionUser1)

    assert(copies.size == 1)
    assert(copies.head.workflow.getName == "dup-src_copy")
    assert(copies.head.workflow.getWid != wid) // a new workflow, not the original
    val names =
      workflowResource.retrieveWorkflowsBySessionUser(sessionUser1).map(_.workflow.getName)
    assert(names.contains("dup-src") && names.contains("dup-src_copy"))
  }

  // ─── shared-access and failure paths (issue #7591) ──────────────────────────

  // Content with an operators array: duplicate/clone reassign operator ids and fail
  // on content that has none.
  private val contentWithOperator =
    "{\"operators\":[{\"operatorID\":\"op1\",\"operatorType\":\"CSVFileScan\"}]}"

  private def grantAccess(wid: Integer, user: User, privilege: PrivilegeEnum): Unit =
    new WorkflowUserAccessDao(getDSLContext.configuration())
      .merge(new WorkflowUserAccess(user.getUid, wid, privilege))

  // A request whose remote address is a valid IPv4 so recordCloneAction stores it.
  private def cloneRequest: HttpServletRequest = {
    val r = stub[HttpServletRequest]
    (r.getRemoteAddr _).when().returns("127.0.0.1")
    r
  }

  private def workflowNamesOf(user: SessionUser): List[String] =
    workflowResource.retrieveWorkflowsBySessionUser(user).map(_.workflow.getName)

  private def versionCount(wid: Integer): Int =
    getDSLContext.fetchCount(WORKFLOW_VERSION, WORKFLOW_VERSION.WID.eq(wid))

  "WorkflowResource.getWorkflowName (companion)" should "return the stored name" in {
    val wid = seedWorkflow(sessionUser1, "companion-name-wf").workflow.getWid
    assert(WorkflowResource.getWorkflowName(wid) == "companion-name-wf")
  }

  it should "throw NotFoundException for a wid that does not exist" in {
    val wid = seedWorkflow(sessionUser1, "companion-missing-wf").workflow.getWid
    assertThrows[NotFoundException](WorkflowResource.getWorkflowName(wid + 100000))
  }

  "WorkflowResource.persistWorkflow" should "update the workflow in place for its owner and record a version" in {
    val workflow = seedWorkflow(sessionUser1, "persist-owner", "d", "{\"a\":1}").workflow
    val versionsBefore = versionCount(workflow.getWid)
    workflow.setContent("{\"a\":2}")

    val persisted = workflowResource.persistWorkflow(workflow, sessionUser1)

    assert(persisted.getContent == "{\"a\":2}")
    assert(workflowResource.retrieveWorkflow(workflow.getWid, sessionUser1).content == "{\"a\":2}")
    assert(versionCount(workflow.getWid) == versionsBefore + 1)
    // updating must not create a second workflow
    assert(workflowNamesOf(sessionUser1) == List("persist-owner"))
  }

  it should "let a non-owner with write access update the workflow and record a version" in {
    val workflow = seedWorkflow(sessionUser1, "persist-writer", "d", "{\"a\":1}").workflow
    grantAccess(workflow.getWid, testUser2, PrivilegeEnum.WRITE)
    val versionsBefore = versionCount(workflow.getWid)
    workflow.setContent("{\"a\":3}")

    workflowResource.persistWorkflow(workflow, sessionUser2)

    assert(workflowResource.retrieveWorkflow(workflow.getWid, sessionUser1).content == "{\"a\":3}")
    assert(versionCount(workflow.getWid) == versionsBefore + 1)
    // Guards rather than pins: both already hold before persistWorkflow is called, since
    // seedWorkflow and grantAccess establish them and the write-access branch touches only
    // WORKFLOW and WORKFLOW_VERSION. They document the intent -- the writer updates the owner's
    // workflow rather than getting a copy of its own -- while the two assertions above carry the
    // actual pin.
    assert(workflowResource.getOwnerName(workflow.getWid) == testUser.getName)
    assert(workflowNamesOf(sessionUser2) == List("persist-writer"))
  }

  it should "reject a non-owner with only read access" in {
    val workflow = seedWorkflow(sessionUser1, "persist-reader", "d", "{\"a\":1}").workflow
    grantAccess(workflow.getWid, testUser2, PrivilegeEnum.READ)
    workflow.setContent("{\"a\":9}")

    assertThrows[ForbiddenException](workflowResource.persistWorkflow(workflow, sessionUser2))
    assert(workflowResource.retrieveWorkflow(workflow.getWid, sessionUser1).content == "{\"a\":1}")
  }

  it should "reject a user with no access to an existing workflow" in {
    val workflow = seedWorkflow(sessionUser1, "persist-no-access", "d", "{\"a\":1}").workflow
    workflow.setContent("{\"a\":9}")

    assertThrows[ForbiddenException](workflowResource.persistWorkflow(workflow, sessionUser2))
    assert(workflowResource.retrieveWorkflow(workflow.getWid, sessionUser1).content == "{\"a\":1}")
    // the rejected persist must not silently create a copy owned by the caller
    assert(workflowNamesOf(sessionUser2).isEmpty)
  }

  "WorkflowResource.createWorkflow" should "reject a workflow that already carries an id" in {
    val existing = seedWorkflow(sessionUser1, "already-has-id").workflow

    assertThrows[BadRequestException](workflowResource.createWorkflow(existing, sessionUser1))
    assert(workflowNamesOf(sessionUser1) == List("already-has-id"))
  }

  "WorkflowResource.updateWorkflowName" should "accept a non-owner with write access" in {
    val wid = seedWorkflow(sessionUser1, "writer-rename").workflow.getWid
    grantAccess(wid, testUser2, PrivilegeEnum.WRITE)
    val update = new Workflow()
    update.setWid(wid)
    update.setName("renamed-by-writer")

    workflowResource.updateWorkflowName(update, sessionUser2)

    assert(workflowResource.getWorkflowName(wid) == "renamed-by-writer")
  }

  it should "reject a user with neither ownership nor write access" in {
    val wid = seedWorkflow(sessionUser1, "no-access-rename").workflow.getWid
    val update = new Workflow()
    update.setWid(wid)
    update.setName("should-not-apply")

    assertThrows[ForbiddenException](workflowResource.updateWorkflowName(update, sessionUser2))
    assert(workflowResource.getWorkflowName(wid) == "no-access-rename")
  }

  "WorkflowResource.makePrivate" should "reject a user without write access" in {
    val wid = seedWorkflow(sessionUser1, "private-forbidden").workflow.getWid
    workflowResource.makePublic(wid, sessionUser1)

    assertThrows[ForbiddenException](workflowResource.makePrivate(wid, sessionUser2))
    assert(workflowResource.getWorkflowType(wid) == "Public")
  }

  "WorkflowResource.cloneWorkflow" should "copy the workflow to the caller and record the clone" in {
    // The source is made public because that is the flow this endpoint serves: the hub's clone
    // button, on someone else's published workflow.
    val wid = seedWorkflow(sessionUser1, "clone-src", "d", contentWithOperator).workflow.getWid
    workflowResource.makePublic(wid, sessionUser1)

    val newWid = workflowResource.cloneWorkflow(wid, sessionUser2, cloneRequest)

    assert(newWid != wid)
    val clone = workflowResource.retrieveWorkflow(newWid, sessionUser2)
    assert(clone.name == "clone-src_clone")
    assert(clone.description == "d")
    assert(!clone.content.contains("\"op1\"")) // operator ids are reassigned
    assert(clone.content.contains("\"CSVFileScan\""))
    assert(!clone.isPublished) // the clone starts private; the source's publicness is not inherited
    // the clone belongs to the caller, not to the original owner
    assert(workflowResource.getOwnerName(newWid) == testUser2.getName)
    assert(
      getDSLContext.fetchCount(
        WORKFLOW_USER_CLONES,
        WORKFLOW_USER_CLONES.WID.eq(wid).and(WORKFLOW_USER_CLONES.UID.eq(testUser2.getUid))
      ) == 1
    )
  }

  it should "clone a private workflow the caller has been granted read access to" in {
    val wid = seedWorkflow(sessionUser1, "clone-shared", "d", contentWithOperator).workflow.getWid
    grantAccess(wid, testUser2, PrivilegeEnum.READ)

    val newWid = workflowResource.cloneWorkflow(wid, sessionUser2, cloneRequest)

    assert(workflowResource.retrieveWorkflow(newWid, sessionUser2).name == "clone-shared_clone")
  }

  it should "reject a caller with no access to the source workflow" in {
    val wid =
      seedWorkflow(sessionUser1, "clone-forbidden", "d", contentWithOperator).workflow.getWid

    assertThrows[ForbiddenException](
      workflowResource.cloneWorkflow(wid, sessionUser2, cloneRequest)
    )

    // no copy reached the caller, and the rejected attempt was not recorded as a clone
    assert(workflowNamesOf(sessionUser2).isEmpty)
    assert(getDSLContext.fetchCount(WORKFLOW_USER_CLONES, WORKFLOW_USER_CLONES.WID.eq(wid)) == 0)
  }

  "WorkflowResource.duplicateWorkflow" should "wrap a failure raised while copying the workflow in a WebApplicationException" in {
    // "{}" has no operators array, so assignNewOperatorIds throws.
    //
    // Note what this does NOT pin: transactionality. assignNewOperatorIds fails before
    // createWorkflow inserts anything, so "no copy was created" holds whether or not the body runs
    // in a transaction -- replacing `context.transaction { ... }` with a plain block leaves this
    // test green. Pinning the rollback would need a failure raised after the insert, and there is
    // no seam for one.
    val wid = seedWorkflow(sessionUser1, "dup-no-operators").workflow.getWid

    val thrown = intercept[WebApplicationException](
      workflowResource.duplicateWorkflow(WorkflowIDs(List(wid)), sessionUser1)
    )

    // not a ForbiddenException/BadRequestException, which the same catch swallows
    assert(thrown.getClass == classOf[WebApplicationException])
    assert(thrown.getCause.isInstanceOf[NoSuchElementException])
    assert(workflowNamesOf(sessionUser1) == List("dup-no-operators"))
  }

  "WorkflowResource.deleteWorkflow" should "wrap an unexpected failure in a WebApplicationException" in {
    // A request body without a "wids" field deserializes to a null list.
    val thrown = intercept[WebApplicationException](
      workflowResource.deleteWorkflow(WorkflowIDs(null), sessionUser1)
    )

    assert(thrown.getClass == classOf[WebApplicationException])
    assert(thrown.getCause.isInstanceOf[NullPointerException])
  }

  // What this pins, and what it does not. It DOES pin that a URI which cannot be decoded is
  // tolerated rather than aborting the delete: removing the `case NonFatal(exception) =>` arm of
  // deleteWorkflow's outer catch turns this test red.
  //
  // It does NOT pin the post-transaction cleanup tail it happens to execute. `LargeBinaryManager`
  // is an object talking to S3 with no injectable seam, and the document cleanup needs real
  // Iceberg-backed documents this spec has no fixture for -- emptying the collected execution ids
  // leaves the suite green. Those lines are entered, not verified.
  it should "still delete the workflow when a stored execution URI cannot be decoded" in {
    val wid = seedWorkflow(sessionUser1, "undecodable-uri-wf").workflow.getWid
    getDSLContext
      .insertInto(WORKFLOW_EXECUTIONS)
      .set(WORKFLOW_EXECUTIONS.VID, WorkflowVersionResource.getLatestVersion(wid))
      .set(WORKFLOW_EXECUTIONS.UID, testUser.getUid)
      .set(WORKFLOW_EXECUTIONS.ENVIRONMENT_VERSION, "test-env")
      .set(WORKFLOW_EXECUTIONS.RUNTIME_STATS_URI, "bogus://not-a-vfs-uri")
      .execute()

    workflowResource.deleteWorkflow(WorkflowIDs(List(wid)), sessionUser1)

    assert(workflowNamesOf(sessionUser1).isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Form View: the per-workflow default view (canvas or form).
  // ---------------------------------------------------------------------------

  // duplicateWorkflow runs assignNewOperatorIds over the content, which requires a
  // real `operators` array, so the toy content used elsewhere in this spec won't do.
  private val contentWithOperators =
    """{"operators":[{"operatorID":"Limit-operator-1","operatorType":"Limit"}],""" +
      """"operatorPositions":{},"links":[],"commentBoxes":[],"settings":{}}"""

  /** Persist a fresh workflow owned by user 1 and return its wid. */
  private def persistFreshWorkflow(
      name: String,
      content: String = contentWithOperators
  ): Integer = {
    val workflow = new Workflow()
    workflow.setName(name)
    workflow.setContent(content)
    workflowResource.persistWorkflow(workflow, sessionUser1)
    workflow.getWid
  }

  private def defaultView(wid: Integer): DefaultViewEnum =
    getDSLContext
      .select(WORKFLOW.DEFAULT_VIEW)
      .from(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .fetchOne()
      .value1()

  private def contentOf(wid: Integer): String =
    getDSLContext
      .select(WORKFLOW.CONTENT)
      .from(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .fetchOne()
      .value1()

  private def lastModifiedOf(wid: Integer): Timestamp =
    getDSLContext
      .select(WORKFLOW.LAST_MODIFIED_TIME)
      .from(WORKFLOW)
      .where(WORKFLOW.WID.eq(wid))
      .fetchOne()
      .value1()

  "/set-default-view API" should "switch the default view to form and back to canvas" in {
    val wid = persistFreshWorkflow("param_toggle")
    assert(defaultView(wid) == DefaultViewEnum.CANVAS, "a new workflow must default to the canvas")

    workflowResource.setDefaultView(wid, DefaultViewRequest("FORM"), sessionUser1)
    assert(defaultView(wid) == DefaultViewEnum.FORM)

    workflowResource.setDefaultView(wid, DefaultViewRequest("CANVAS"), sessionUser1)
    assert(defaultView(wid) == DefaultViewEnum.CANVAS)
  }

  it should "reject a user without write access" in {
    val wid = persistFreshWorkflow("param_no_access")

    assertThrows[ForbiddenException] {
      workflowResource.setDefaultView(wid, DefaultViewRequest("FORM"), sessionUser2)
    }
    assert(defaultView(wid) == DefaultViewEnum.CANVAS)
  }

  it should "reject an invalid or missing view value" in {
    val wid = persistFreshWorkflow("param_invalid")

    assertThrows[BadRequestException] {
      workflowResource.setDefaultView(wid, DefaultViewRequest("SIDEBAR"), sessionUser1)
    }
    // A missing/null body value must be a 400, not a 500 (lookupLiteral returns null, not NPE).
    assertThrows[BadRequestException] {
      workflowResource.setDefaultView(wid, DefaultViewRequest(null), sessionUser1)
    }
    assert(defaultView(wid) == DefaultViewEnum.CANVAS)
  }

  // A plain save (persistWorkflow) only writes the fields the client sends -- name,
  // description, content, is_public -- and never `default_view`, so saving the canvas must
  // not reset the default view. The edit payload mirrors what the frontend sends.
  it should "survive a subsequent save of the workflow" in {
    val wid = persistFreshWorkflow("param_survives_save")
    workflowResource.setDefaultView(wid, DefaultViewRequest("FORM"), sessionUser1)

    val edit = new Workflow()
    edit.setWid(wid)
    edit.setName("param_survives_save_edited")
    edit.setContent("{\"operators\":[],\"links\":[]}")
    edit.setIsPublic(false)
    workflowResource.persistWorkflow(edit, sessionUser1)

    assert(
      defaultView(wid) == DefaultViewEnum.FORM,
      "saving the canvas must not reset the default view"
    )
  }

  // A biologist's path is hub -> clone -> use, so a copy has to stay usable.
  it should "be inherited by a duplicated workflow" in {
    val wid = persistFreshWorkflow("param_source")
    workflowResource.setDefaultView(wid, DefaultViewRequest("FORM"), sessionUser1)

    val copies = workflowResource.duplicateWorkflow(WorkflowIDs(List(wid)), sessionUser1)

    assert(copies.length == 1)
    assert(
      defaultView(copies.head.workflow.getWid) == DefaultViewEnum.FORM,
      "the copy must keep the preference"
    )
  }

  // The hub's clone button goes through cloneWorkflow (not duplicateWorkflow); a cloned
  // form-default workflow must stay form-default so the copy opens straight into its form.
  it should "be inherited by a workflow cloned through cloneWorkflow" in {
    val wid =
      seedWorkflow(sessionUser1, "clone-formview-src", "d", contentWithOperator).workflow.getWid
    workflowResource.makePublic(wid, sessionUser1)
    workflowResource.setDefaultView(wid, DefaultViewRequest("FORM"), sessionUser1)

    val newWid = workflowResource.cloneWorkflow(wid, sessionUser2, cloneRequest)

    assert(defaultView(newWid) == DefaultViewEnum.FORM, "the clone must keep the default view")
  }

  // Both views load a workflow through this endpoint, and the client needs the default view
  // to know which one to open first. Leaving the value out of the payload left the client
  // guessing, so it is worth pinning down.
  it should "be reported by the endpoint both views load through" in {
    val wid = persistFreshWorkflow("param_retrieve")
    assert(
      workflowResource.retrieveWorkflow(wid, sessionUser1).defaultView == DefaultViewEnum.CANVAS
    )

    workflowResource.setDefaultView(wid, DefaultViewRequest("FORM"), sessionUser1)

    assert(workflowResource.retrieveWorkflow(wid, sessionUser1).defaultView == DefaultViewEnum.FORM)
  }

  it should "leave a duplicate of a plain workflow defaulting to the canvas" in {
    val wid = persistFreshWorkflow("plain_source")

    val copies = workflowResource.duplicateWorkflow(WorkflowIDs(List(wid)), sessionUser1)

    assert(copies.length == 1)
    assert(defaultView(copies.head.workflow.getWid) == DefaultViewEnum.CANVAS)
  }

  // Setting the preference updates only its own column, so a mere change must not bump the
  // workflow's last-modified time (which would reorder the dashboard's "recent" listing).
  it should "not change last_modified_time when the default view is set" in {
    val wid = persistFreshWorkflow("param_mtime")
    val before = lastModifiedOf(wid)

    workflowResource.setDefaultView(wid, DefaultViewRequest("FORM"), sessionUser1)
    assert(
      lastModifiedOf(wid) == before,
      "setting the default view must not bump last_modified_time"
    )

    workflowResource.setDefaultView(wid, DefaultViewRequest("CANVAS"), sessionUser1)
    assert(lastModifiedOf(wid) == before, "setting it back must not bump last_modified_time")
  }

  // The dashboard listing (GET /workflow/list) selects specific columns, so it has to include
  // default_view explicitly or every listed workflow would report the POJO default (null).
  it should "be reported by the workflow listing endpoint" in {
    val wid = persistFreshWorkflow("param_list")
    workflowResource.setDefaultView(wid, DefaultViewRequest("FORM"), sessionUser1)

    val listed =
      workflowResource.retrieveWorkflowsBySessionUser(sessionUser1).find(_.workflow.getWid == wid)

    assert(listed.isDefined)
    assert(
      listed.get.workflow.getDefaultView == DefaultViewEnum.FORM,
      "the listing must carry the default view"
    )
  }

  // The hub loads a public workflow through retrievePublicWorkflow, and a clone opens
  // straight into the form only when that response says the source defaults to the form.
  it should "be reported by retrievePublicWorkflow for a public workflow" in {
    val workflow = new Workflow()
    workflow.setName("param_public_retrieve")
    workflow.setContent(contentWithOperators)
    workflow.setIsPublic(true)
    workflowResource.persistWorkflow(workflow, sessionUser1)
    val wid = workflow.getWid

    assert(workflowResource.retrievePublicWorkflow(wid).defaultView == DefaultViewEnum.CANVAS)

    workflowResource.setDefaultView(wid, DefaultViewRequest("FORM"), sessionUser1)

    assert(workflowResource.retrievePublicWorkflow(wid).defaultView == DefaultViewEnum.FORM)
  }

  // Switching the default back to canvas only changes the preference; the author's setup lives
  // in content under `formBinding` and must survive so switching back to form restores it.
  it should "keep the form definition in content when the default view is set back to canvas" in {
    val withBinding =
      """{"operators":[{"operatorID":"Limit-operator-1","operatorType":"Limit"}],""" +
        """"operatorPositions":{},"links":[],"commentBoxes":[],"settings":{},""" +
        """"formBinding":{"exposed":["Limit-operator-1"]}}"""
    val wid = persistFreshWorkflow("param_keep_def", withBinding)
    workflowResource.setDefaultView(wid, DefaultViewRequest("FORM"), sessionUser1)

    workflowResource.setDefaultView(wid, DefaultViewRequest("CANVAS"), sessionUser1)

    assert(defaultView(wid) == DefaultViewEnum.CANVAS)
    assert(
      contentOf(wid).contains("formBinding"),
      "switching back to canvas must not erase the form definition"
    )
  }

}
