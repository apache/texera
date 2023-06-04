package edu.uci.ics.texera.web.resource.dashboard.file

import edu.uci.ics.texera.web.MockTexeraDB
import edu.uci.ics.texera.web.auth.SessionUser
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{User, Workflow}
import org.jooq.types.UInteger
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserRole
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflowEntry
import org.jooq.Condition
import org.jooq.impl.DSL.noCondition
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{USER, WORKFLOW, WORKFLOW_OF_PROJECT}

import java.util.concurrent.TimeUnit
import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}
import java.util
import java.util.Collections

class WorkflowResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val testUser: User = {
    val user = new User
    user.setUid(UInteger.valueOf(1))
    user.setName("test_user")
    user.setRole(UserRole.ADMIN)
    user.setPassword("123")
    user
  }

  private val testUser2: User = {
    val user = new User
    user.setUid(UInteger.valueOf(2))
    user.setName("test_user2")
    user.setRole(UserRole.ADMIN)
    user.setPassword("123")
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

  private var workflowResource: WorkflowResource = {
    new WorkflowResource()
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    // build fulltext indexes
    val fulltextIndexPath = {
      Utils.amberHomePath.resolve("../scripts/sql/update/fulltext_indexes.sql").toRealPath()
    }
    executeScriptInJDBC(fulltextIndexPath)

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
    for (workflow <- workflows) {
      workflowResource.deleteWorkflow(workflow.workflow.getWid(), sessionUser1)
    }
    workflows = workflowResource.retrieveWorkflowsBySessionUser(sessionUser2)
    for (workflow <- workflows) {
      workflowResource.deleteWorkflow(workflow.workflow.getWid(), sessionUser2)
    }
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  private def getKeywordsArray(keyword: String): util.ArrayList[String] = {
    val keywords = new util.ArrayList[String]()
    keywords.add(keyword)

    keywords
  }
  private def assertSameWorkflow(a: Workflow, b: DashboardWorkflowEntry): Unit = {
    assert(a.getName == b.workflow.getName)
  }

  "/search API " should "be able to search for workflows in different columns in Workflow table" in {
    // testWorkflow1: {name: test_name, descrption: test_description, content: test_content}
    // search "test_name" or "test_description" or "test_content" should return testWorkflow1
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    // search
    var DashboardWorkflowEntryList =
      workflowResource.searchWorkflows(sessionUser1, getKeywordsArray(keywordInWorkflow1Content))
    assert(DashboardWorkflowEntryList.head.ownerName.equals(testUser.getName))
    assert(DashboardWorkflowEntryList.length == 1)
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList.head)
    DashboardWorkflowEntryList =
      workflowResource.searchWorkflows(sessionUser1, getKeywordsArray(keywordInWorkflow1Content))
    assert(DashboardWorkflowEntryList.head.ownerName.equals(testUser.getName))
    assert(DashboardWorkflowEntryList.length == 1)
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList.head)
  }

  it should "be able to search text phrases" in {
    // testWorkflow1: {name: "test_name", descrption: "test_description", content: "text phrase"}
    // search "text phrase" should return testWorkflow1
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    val DashboardWorkflowEntryList =
      workflowResource.searchWorkflows(sessionUser1, getKeywordsArray(keywordInWorkflow1Content))
    assert(DashboardWorkflowEntryList.length == 1)
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList.head)
    val DashboardWorkflowEntryList1 =
      workflowResource.searchWorkflows(sessionUser1, getKeywordsArray("text sear"))
    assert(DashboardWorkflowEntryList1.length == 0)
  }

  it should "return an all workflows when given an empty list of keywords" in {
    // search "" should return all workflows
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    // search with empty keywords
    val keywords = new util.ArrayList[String]()
    val DashboardWorkflowEntryList = workflowResource.searchWorkflows(sessionUser1, keywords)
    assert(DashboardWorkflowEntryList.length == 2)
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
    val DashboardWorkflowEntryList = workflowResource.searchWorkflows(sessionUser1, keywords)
    assert(DashboardWorkflowEntryList.size == 1)
    assert(DashboardWorkflowEntryList.head.ownerName.equals(testUser.getName))
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList.head)

    keywords.add("nonexistent")
    val DashboardWorkflowEntryList2 = workflowResource.searchWorkflows(sessionUser1, keywords)
    assert(DashboardWorkflowEntryList2.isEmpty)

    val keywordsReverseOrder = new util.ArrayList[String]()
    keywordsReverseOrder.add(testWorkflow1.getDescription)
    keywordsReverseOrder.add(keywordInWorkflow1Content)
    val DashboardWorkflowEntryList1 =
      workflowResource.searchWorkflows(sessionUser1, keywordsReverseOrder)
    assert(DashboardWorkflowEntryList1.size == 1)
    assert(DashboardWorkflowEntryList1.head.ownerName.equals(testUser.getName))
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList1.head)

  }

  it should "handle reserved characters in the keywords" in {
    // testWorkflow1: {name: test_name, description: test_description, content: "key pair"}
    // search "key+-pair" or "key@pair" or "key+" or "+key" should return testWorkflow1
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    // search with reserved characters in keywords
    var DashboardWorkflowEntryList = workflowResource.searchWorkflows(
      sessionUser1,
      getKeywordsArray(keywordInWorkflow1Content + "+-@()<>~*\"" + keywordInWorkflow1Content)
    )
    assert(DashboardWorkflowEntryList.size == 1)
    assert(DashboardWorkflowEntryList.head.ownerName.equals(testUser.getName))
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList.head)

    DashboardWorkflowEntryList = workflowResource.searchWorkflows(
      sessionUser1,
      getKeywordsArray(keywordInWorkflow1Content + "@" + keywordInWorkflow1Content)
    )
    assert(DashboardWorkflowEntryList.size == 1)
    assert(DashboardWorkflowEntryList.head.ownerName.equals(testUser.getName))
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList.head)

    DashboardWorkflowEntryList = workflowResource.searchWorkflows(
      sessionUser1,
      getKeywordsArray(keywordInWorkflow1Content + "+-@()<>~*\"")
    )
    assert(DashboardWorkflowEntryList.size == 1)
    assert(DashboardWorkflowEntryList.head.ownerName.equals(testUser.getName))
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList.head)

    DashboardWorkflowEntryList = workflowResource.searchWorkflows(
      sessionUser1,
      getKeywordsArray("+-@()<>~*\"" + keywordInWorkflow1Content)
    )
    assert(DashboardWorkflowEntryList.size == 1)
    assert(DashboardWorkflowEntryList.head.ownerName.equals(testUser.getName))
    assertSameWorkflow(testWorkflow1, DashboardWorkflowEntryList.head)

  }

  it should "return all workflows when keywords only contains reserved keywords +-@()<>~*\"" in {
    // search "+-@()<>~*"" should return all workflows
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)

    val DashboardWorkflowEntryList =
      workflowResource.searchWorkflows(sessionUser1, getKeywordsArray("+-@()<>~*\""))
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
        workflowResource.searchWorkflows(user, getKeywordsArray(workflow.getDescription))
      assert(DashboardWorkflowEntryList.size == 1)
      assert(DashboardWorkflowEntryList.head.ownerName.equals(user.getName()))
      assertSameWorkflow(workflow, DashboardWorkflowEntryList.head)
    }
    test(sessionUser1, testWorkflow1)
    test(sessionUser2, testWorkflow2)
  }

  it should "search for keywords containing special characters" in {
    // testWorkflow1: {name: test_name, description: test_description, content: "key@gmail.com"}
    // search "key@gmail.com" should return testWorkflow1
    workflowResource.persistWorkflow(testWorkflowWithSpecialCharacters, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    val DashboardWorkflowEntryList =
      workflowResource.searchWorkflows(sessionUser1, getKeywordsArray(exampleEmailAddress))
    assert(DashboardWorkflowEntryList.size == 1)
    assertSameWorkflow(testWorkflowWithSpecialCharacters, DashboardWorkflowEntryList.head)
  }

  it should "be case insensitive" in {
    // testWorkflow1: {name: test_name, description: test_description, content: "key pair"}
    // search ["key", "pair] or ["KEY", "PAIR"] should return the same result
    workflowResource.persistWorkflow(testWorkflowWithSpecialCharacters, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    val keywords = new util.ArrayList[String]()
    keywords.add(exampleWord1.toLowerCase())
    keywords.add(exampleWord2.toUpperCase())
    val DashboardWorkflowEntryList = workflowResource.searchWorkflows(sessionUser1, keywords)
    assert(DashboardWorkflowEntryList.size == 1)
    assertSameWorkflow(testWorkflowWithSpecialCharacters, DashboardWorkflowEntryList.head)
  }

  it should "be order insensitive" in {
    // testWorkflow1: {name: test_name, description: test_description, content: "key pair"}
    // search ["key", "pair] or ["pair", "key"] should return the same result
    workflowResource.persistWorkflow(testWorkflowWithSpecialCharacters, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    val keywords = new util.ArrayList[String]()
    keywords.add(exampleWord2)
    keywords.add(exampleWord1)
    val DashboardWorkflowEntryList = workflowResource.searchWorkflows(sessionUser1, keywords)
    assert(DashboardWorkflowEntryList.size == 1)
    assertSameWorkflow(testWorkflowWithSpecialCharacters, DashboardWorkflowEntryList.head)
  }

  "getOwnerFilter" should "return a noCondition when the input owner list is null" in {
    val ownerFilter: Condition = workflowResource.getOwnerFilter(null)
    assert(ownerFilter.toString == noCondition().toString)
  }

  it should "return a noCondition when the input owner list is empty" in {
    val ownerFilter: Condition = workflowResource.getOwnerFilter(Collections.emptyList[String]())
    assert(ownerFilter.toString == noCondition().toString)
  }

  it should "return a proper condition for a single owner" in {
    val ownerList = new java.util.ArrayList[String](util.Arrays.asList("owner1"))
    val ownerFilter: Condition = workflowResource.getOwnerFilter(ownerList)
    assert(ownerFilter.toString == USER.EMAIL.eq("owner1").toString)
  }

  it should "return a proper condition for multiple owners" in {
    val ownerList = new java.util.ArrayList[String](util.Arrays.asList("owner1", "owner2"))
    val ownerFilter: Condition = workflowResource.getOwnerFilter(ownerList)
    assert(ownerFilter.toString == USER.EMAIL.eq("owner1").or(USER.EMAIL.eq("owner2")).toString)
  }

  it should "return a proper condition for multiple owners with duplicates" in {
    val ownerList =
      new java.util.ArrayList[String](util.Arrays.asList("owner1", "owner2", "owner2"))
    val ownerFilter: Condition = workflowResource.getOwnerFilter(ownerList)
    assert(ownerFilter.toString == USER.EMAIL.eq("owner1").or(USER.EMAIL.eq("owner2")).toString)
  }

  "getProjectFilter" should "return a noCondition when the input projectIds list is null" in {
    val projectFilter: Condition = workflowResource.getProjectFilter(null)
    assert(projectFilter.toString == noCondition().toString)
  }

  it should "return a noCondition when the input projectIds list is empty" in {
    val projectFilter: Condition =
      workflowResource.getProjectFilter(Collections.emptyList[UInteger]())
    assert(projectFilter.toString == noCondition().toString)
  }

  it should "return a proper condition for a single projectId" in {
    val projectIdList = new java.util.ArrayList[UInteger](util.Arrays.asList(UInteger.valueOf(1)))
    val projectFilter: Condition = workflowResource.getProjectFilter(projectIdList)
    assert(projectFilter.toString == WORKFLOW_OF_PROJECT.PID.eq(UInteger.valueOf(1)).toString)
  }

  it should "return a proper condition for multiple projectIds" in {
    val projectIdList = new java.util.ArrayList[UInteger](
      util.Arrays.asList(UInteger.valueOf(1), UInteger.valueOf(2))
    )
    val projectFilter: Condition = workflowResource.getProjectFilter(projectIdList)
    assert(
      projectFilter.toString == WORKFLOW_OF_PROJECT.PID
        .eq(UInteger.valueOf(1))
        .or(WORKFLOW_OF_PROJECT.PID.eq(UInteger.valueOf(2)))
        .toString
    )
  }

  it should "return a proper condition for multiple projectIds with duplicates" in {
    val projectIdList = new java.util.ArrayList[UInteger](
      util.Arrays.asList(UInteger.valueOf(1), UInteger.valueOf(2), UInteger.valueOf(2))
    )
    val projectFilter: Condition = workflowResource.getProjectFilter(projectIdList)
    assert(
      projectFilter.toString == WORKFLOW_OF_PROJECT.PID
        .eq(UInteger.valueOf(1))
        .or(WORKFLOW_OF_PROJECT.PID.eq(UInteger.valueOf(2)))
        .toString
    )
  }

  "getWorkflowIdFilter" should "return a noCondition when the input workflowIDs list is null" in {
    val workflowIdFilter: Condition = workflowResource.getWorkflowIdFilter(null)
    assert(workflowIdFilter.toString == noCondition().toString)
  }

  it should "return a noCondition when the input workflowIDs list is empty" in {
    val workflowIdFilter: Condition =
      workflowResource.getWorkflowIdFilter(Collections.emptyList[UInteger]())
    assert(workflowIdFilter.toString == noCondition().toString)
  }

  it should "return a proper condition for a single workflowID" in {
    val workflowIdList = new java.util.ArrayList[UInteger](util.Arrays.asList(UInteger.valueOf(1)))
    val workflowIdFilter: Condition = workflowResource.getWorkflowIdFilter(workflowIdList)
    assert(workflowIdFilter.toString == WORKFLOW.WID.eq(UInteger.valueOf(1)).toString)
  }

  it should "return a proper condition for multiple workflowIDs" in {
    val workflowIdList = new java.util.ArrayList[UInteger](
      util.Arrays.asList(UInteger.valueOf(1), UInteger.valueOf(2))
    )
    val workflowIdFilter: Condition = workflowResource.getWorkflowIdFilter(workflowIdList)
    assert(
      workflowIdFilter.toString == WORKFLOW.WID
        .eq(UInteger.valueOf(1))
        .or(WORKFLOW.WID.eq(UInteger.valueOf(2)))
        .toString
    )
  }

  it should "return a proper condition for multiple workflowIDs with duplicates" in {
    val workflowIdList = new java.util.ArrayList[UInteger](
      util.Arrays.asList(UInteger.valueOf(1), UInteger.valueOf(2), UInteger.valueOf(2))
    )
    val workflowIdFilter: Condition = workflowResource.getWorkflowIdFilter(workflowIdList)
    assert(
      workflowIdFilter.toString == WORKFLOW.WID
        .eq(UInteger.valueOf(1))
        .or(WORKFLOW.WID.eq(UInteger.valueOf(2)))
        .toString
    )
  }

  "getDateFilter" should "return a noCondition when the input startDate and endDate are empty" in {
    val dateFilter: Condition = workflowResource.getDateFilter("creation", "", "")
    assert(dateFilter.toString == noCondition().toString)
  }

  it should "return a proper condition for creation date type with specific start and end date" in {
    val dateFilter: Condition =
      workflowResource.getDateFilter("creation", "2023-01-01", "2023-12-31")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startTimestamp = new Timestamp(dateFormat.parse("2023-01-01").getTime)
    val endTimestamp =
      new Timestamp(dateFormat.parse("2023-12-31").getTime + TimeUnit.DAYS.toMillis(1) - 1)
    assert(
      dateFilter.toString == WORKFLOW.CREATION_TIME.between(startTimestamp, endTimestamp).toString
    )
  }

  it should "return a proper condition for modification date type with specific start and end date" in {
    val dateFilter: Condition =
      workflowResource.getDateFilter("modification", "2023-01-01", "2023-12-31")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val startTimestamp = new Timestamp(dateFormat.parse("2023-01-01").getTime)
    val endTimestamp =
      new Timestamp(dateFormat.parse("2023-12-31").getTime + TimeUnit.DAYS.toMillis(1) - 1)
    assert(
      dateFilter.toString == WORKFLOW.LAST_MODIFIED_TIME
        .between(startTimestamp, endTimestamp)
        .toString
    )
  }

  it should "throw an IllegalArgumentException for invalid dateType" in {
    assertThrows[IllegalArgumentException] {
      workflowResource.getDateFilter("invalidType", "2023-01-01", "2023-12-31")
    }
  }

  it should "throw a ParseException when endDate is invalid" in {
    assertThrows[ParseException] {
      workflowResource.getDateFilter("creation", "2023-01-01", "invalidDate")
    }
  }

  "getOperatorsFilter" should "return a noCondition when the input operators list is empty" in {
    val operatorsFilter: Condition =
      workflowResource.getOperatorsFilter(Collections.emptyList[String]())
    assert(operatorsFilter.toString == noCondition().toString)
  }

  it should "return a proper condition for a single operator" in {
    val operatorsList = new java.util.ArrayList[String](util.Arrays.asList("operator1"))
    val operatorsFilter: Condition = workflowResource.getOperatorsFilter(operatorsList)
    val searchKey = "%\"operatorType\":\"operator1\"%"
    assert(operatorsFilter.toString == WORKFLOW.CONTENT.likeIgnoreCase(searchKey).toString)
  }

  it should "return a proper condition for multiple operators" in {
    val operatorsList =
      new java.util.ArrayList[String](util.Arrays.asList("operator1", "operator2"))
    val operatorsFilter: Condition = workflowResource.getOperatorsFilter(operatorsList)
    val searchKey1 = "%\"operatorType\":\"operator1\"%"
    val searchKey2 = "%\"operatorType\":\"operator2\"%"
    assert(
      operatorsFilter.toString == WORKFLOW.CONTENT
        .likeIgnoreCase(searchKey1)
        .or(WORKFLOW.CONTENT.likeIgnoreCase(searchKey2))
        .toString
    )
  }

}
