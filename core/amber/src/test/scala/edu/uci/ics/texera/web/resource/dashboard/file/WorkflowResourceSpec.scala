package edu.uci.ics.texera.web.resource.dashboard.file

import edu.uci.ics.texera.web.MockTexeraDB
import edu.uci.ics.texera.web.auth.SessionUser
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{User, Workflow}
import org.jooq.types.UInteger
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserRole
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{UserDao, WorkflowDao}
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflowEntry
import org.jooq.Condition
import org.jooq.impl.DSL.noCondition
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{
  PROJECT,
  USER,
  WORKFLOW,
  WORKFLOW_OF_PROJECT,
  WORKFLOW_OF_USER,
  WORKFLOW_USER_ACCESS
}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource

import java.nio.file.Files
import java.nio.charset.StandardCharsets
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
    val db = getDBInstance
    // build fulltext indexes
    val fulltextIndexPath = {
      Utils.amberHomePath.resolve("../scripts/sql/update/fulltext_indexes.sql").toRealPath()
    }
    val buildFulltextIndex =
      new String(Files.readAllBytes(fulltextIndexPath), StandardCharsets.UTF_8)
    db.run(buildFulltextIndex)

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
    assert(ownerFilter.toString == USER.NAME.eq("owner1").toString)
  }

  it should "return a proper condition for multiple owners" in {
    val ownerList = new java.util.ArrayList[String](util.Arrays.asList("owner1", "owner2"))
    val ownerFilter: Condition = workflowResource.getOwnerFilter(ownerList)
    assert(ownerFilter.toString == USER.NAME.eq("owner1").or(USER.NAME.eq("owner2")).toString)
  }

  it should "return a proper condition for multiple owners with duplicates" in {
    val ownerList =
      new java.util.ArrayList[String](util.Arrays.asList("owner1", "owner2", "owner2"))
    val ownerFilter: Condition = workflowResource.getOwnerFilter(ownerList)
    assert(ownerFilter.toString == USER.NAME.eq("owner1").or(USER.NAME.eq("owner2")).toString)
  }

}
