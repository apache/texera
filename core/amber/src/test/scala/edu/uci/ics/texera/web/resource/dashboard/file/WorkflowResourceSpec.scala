package edu.uci.ics.texera.web.resource.dashboard.file

import edu.uci.ics.texera.web.MockTexeraDB
import edu.uci.ics.texera.web.auth.SessionUser
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{Project, User, Workflow}
import org.jooq.types.UInteger
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserRole
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.DashboardWorkflow
import org.jooq.Condition
import org.jooq.impl.DSL.noCondition
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{USER, WORKFLOW, WORKFLOW_OF_PROJECT}
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource
import edu.uci.ics.texera.web.resource.dashboard.user.project.ProjectResource

import java.util.concurrent.TimeUnit
import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}
import java.util
import java.util.Collections
import javax.ws.rs.BadRequestException

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

  private val testProject1: Project = {
    val project = new Project()
    project.setName("test_project1")
    project.setDescription("this is project description")
    project
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

  private val projectResource: ProjectResource = {
    new ProjectResource()
  }

  private val fileResource: UserFileResource = {
    new UserFileResource()
  }

  private val dashboardResource: DashboardResource = {
    new DashboardResource()
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()

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
      workflowResource.deleteWorkflow(workflow.workflow.getWid(), sessionUser1)
    )

    workflows = workflowResource.retrieveWorkflowsBySessionUser(sessionUser2)
    workflows.foreach(workflow =>
      workflowResource.deleteWorkflow(workflow.workflow.getWid(), sessionUser2)
    )

    // delete all projects in the database
    var projects = projectResource.listProjectsOwnedByUser((sessionUser1))
    projects.forEach(project => projectResource.deleteProject(project.getPid(), sessionUser1))

    projects = projectResource.listProjectsOwnedByUser((sessionUser2))
    projects.forEach(project => projectResource.deleteProject(project.getPid(), sessionUser2))

    // delete all files in the database
    var files = fileResource.getFileList(sessionUser1)
    files.forEach(file => fileResource.deleteFile(file.file.getFid(), sessionUser1))

    files = fileResource.getFileList(sessionUser2)
    files.forEach(file => fileResource.deleteFile(file.file.getFid(), sessionUser2))
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  private def getKeywordsArray(keywords: String*): util.ArrayList[String] = {
    val keywordsList = new util.ArrayList[String]()
    for (keyword <- keywords) {
      keywordsList.add(keyword)
    }
    keywordsList
  }
  private def assertSameWorkflow(a: Workflow, b: DashboardWorkflow): Unit = {
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
    val ownerFilter: Condition = WorkflowResource.getOwnerFilter(null)
    assert(ownerFilter.toString == noCondition().toString)
  }

  it should "return a noCondition when the input owner list is empty" in {
    val ownerFilter: Condition = WorkflowResource.getOwnerFilter(Collections.emptyList[String]())
    assert(ownerFilter.toString == noCondition().toString)
  }

  it should "return a proper condition for a single owner" in {
    val ownerList = new java.util.ArrayList[String](util.Arrays.asList("owner1"))
    val ownerFilter: Condition = WorkflowResource.getOwnerFilter(ownerList)
    assert(ownerFilter.toString == USER.EMAIL.eq("owner1").toString)
  }

  it should "return a proper condition for multiple owners" in {
    val ownerList = new java.util.ArrayList[String](util.Arrays.asList("owner1", "owner2"))
    val ownerFilter: Condition = WorkflowResource.getOwnerFilter(ownerList)
    assert(ownerFilter.toString == USER.EMAIL.eq("owner1").or(USER.EMAIL.eq("owner2")).toString)
  }

  it should "return a proper condition for multiple owners with duplicates" in {
    val ownerList =
      new java.util.ArrayList[String](util.Arrays.asList("owner1", "owner2", "owner2"))
    val ownerFilter: Condition = WorkflowResource.getOwnerFilter(ownerList)
    assert(ownerFilter.toString == USER.EMAIL.eq("owner1").or(USER.EMAIL.eq("owner2")).toString)
  }

  "getProjectFilter" should "return a noCondition when the input projectIds list is null" in {
    val projectFilter: Condition = WorkflowResource.getProjectFilter(null)
    assert(projectFilter.toString == noCondition().toString)
  }

  it should "return a noCondition when the input projectIds list is empty" in {
    val projectFilter: Condition =
      WorkflowResource.getProjectFilter(Collections.emptyList[UInteger]())
    assert(projectFilter.toString == noCondition().toString)
  }

  it should "return a proper condition for a single projectId" in {
    val projectIdList = new java.util.ArrayList[UInteger](util.Arrays.asList(UInteger.valueOf(1)))
    val projectFilter: Condition = WorkflowResource.getProjectFilter(projectIdList)
    assert(projectFilter.toString == WORKFLOW_OF_PROJECT.PID.eq(UInteger.valueOf(1)).toString)
  }

  it should "return a proper condition for multiple projectIds" in {
    val projectIdList = new java.util.ArrayList[UInteger](
      util.Arrays.asList(UInteger.valueOf(1), UInteger.valueOf(2))
    )
    val projectFilter: Condition = WorkflowResource.getProjectFilter(projectIdList)
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
    val projectFilter: Condition = WorkflowResource.getProjectFilter(projectIdList)
    assert(
      projectFilter.toString == WORKFLOW_OF_PROJECT.PID
        .eq(UInteger.valueOf(1))
        .or(WORKFLOW_OF_PROJECT.PID.eq(UInteger.valueOf(2)))
        .toString
    )
  }

  "getWorkflowIdFilter" should "return a noCondition when the input workflowIDs list is null" in {
    val workflowIdFilter: Condition = WorkflowResource.getWorkflowIdFilter(null)
    assert(workflowIdFilter.toString == noCondition().toString)
  }

  it should "return a noCondition when the input workflowIDs list is empty" in {
    val workflowIdFilter: Condition =
      WorkflowResource.getWorkflowIdFilter(Collections.emptyList[UInteger]())
    assert(workflowIdFilter.toString == noCondition().toString)
  }

  it should "return a proper condition for a single workflowID" in {
    val workflowIdList = new java.util.ArrayList[UInteger](util.Arrays.asList(UInteger.valueOf(1)))
    val workflowIdFilter: Condition = WorkflowResource.getWorkflowIdFilter(workflowIdList)
    assert(workflowIdFilter.toString == WORKFLOW.WID.eq(UInteger.valueOf(1)).toString)
  }

  it should "return a proper condition for multiple workflowIDs" in {
    val workflowIdList = new java.util.ArrayList[UInteger](
      util.Arrays.asList(UInteger.valueOf(1), UInteger.valueOf(2))
    )
    val workflowIdFilter: Condition = WorkflowResource.getWorkflowIdFilter(workflowIdList)
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
    val workflowIdFilter: Condition = WorkflowResource.getWorkflowIdFilter(workflowIdList)
    assert(
      workflowIdFilter.toString == WORKFLOW.WID
        .eq(UInteger.valueOf(1))
        .or(WORKFLOW.WID.eq(UInteger.valueOf(2)))
        .toString
    )
  }

  "getDateFilter" should "return a noCondition when the input startDate and endDate are empty" in {
    val dateFilter: Condition = WorkflowResource.getDateFilter("creation", "", "", "workflow")
    assert(dateFilter.toString == noCondition().toString)
  }

  it should "return a proper condition for creation date type with specific start and end date" in {
    val dateFilter: Condition =
      WorkflowResource.getDateFilter("creation", "2023-01-01", "2023-12-31", "workflow")
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
      WorkflowResource.getDateFilter("modification", "2023-01-01", "2023-12-31", "workflow")
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
      WorkflowResource.getDateFilter("invalidType", "2023-01-01", "2023-12-31", "workflow")
    }
  }

  it should "throw a ParseException when endDate is invalid" in {
    assertThrows[ParseException] {
      WorkflowResource.getDateFilter("creation", "2023-01-01", "invalidDate", "workflow")
    }
  }

  "getOperatorsFilter" should "return a noCondition when the input operators list is empty" in {
    val operatorsFilter: Condition =
      WorkflowResource.getOperatorsFilter(Collections.emptyList[String]())
    assert(operatorsFilter.toString == noCondition().toString)
  }

  it should "return a proper condition for a single operator" in {
    val operatorsList = new java.util.ArrayList[String](util.Arrays.asList("operator1"))
    val operatorsFilter: Condition = WorkflowResource.getOperatorsFilter(operatorsList)
    val searchKey = "%\"operatorType\":\"operator1\"%"
    assert(operatorsFilter.toString == WORKFLOW.CONTENT.likeIgnoreCase(searchKey).toString)
  }

  it should "return a proper condition for multiple operators" in {
    val operatorsList =
      new java.util.ArrayList[String](util.Arrays.asList("operator1", "operator2"))
    val operatorsFilter: Condition = WorkflowResource.getOperatorsFilter(operatorsList)
    val searchKey1 = "%\"operatorType\":\"operator1\"%"
    val searchKey2 = "%\"operatorType\":\"operator2\"%"
    assert(
      operatorsFilter.toString == WORKFLOW.CONTENT
        .likeIgnoreCase(searchKey1)
        .or(WORKFLOW.CONTENT.likeIgnoreCase(searchKey2))
        .toString
    )
  }

  "/search API" should "be able to search for resources in different tables" in {

    // create different types of resources, project, workflow, and file
    projectResource.createProject(sessionUser1, "test project1")
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    val fileResource = new UserFileResource()
    val in = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    val filename = "test.csv"
    val response = fileResource.uploadFile(
      in,
      filename,
      sessionUser1
    )
    assert(response.getStatusInfo.getStatusCode == 200)
    // search
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResources(sessionUser1, getKeywordsArray("test"))
    assert(DashboardClickableFileEntryList.results.length == 3)

  }

  it should "return an empty list when there are no matching resources" in {
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResources(sessionUser1, getKeywordsArray("not-existing-keyword"))
    assert(DashboardClickableFileEntryList.results.isEmpty)
  }

  it should "return all resources when no keyword provided" in {

    projectResource.createProject(sessionUser1, "test project1")
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResources(sessionUser1, getKeywordsArray(""))
    assert(DashboardClickableFileEntryList.results.length == 2)
  }

  it should "only return resources that match the given keyword" in {
    projectResource.createProject(sessionUser1, "test project")
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    val in = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    val uniqueFilename = "unique.csv"
    val response = fileResource.uploadFile(
      in,
      uniqueFilename,
      sessionUser1
    )
    assert(response.getStatusInfo.getStatusCode == 200)

    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResources(sessionUser1, getKeywordsArray("unique"))
    assert(DashboardClickableFileEntryList.results.length == 1)
  }

  it should "return multiple matching resources from a single resource type" in {
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    projectResource.createProject(sessionUser1, "common project1")
    projectResource.createProject(sessionUser1, "common project2")
    val in = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    val uniqueFilename = "test.csv"
    val response = fileResource.uploadFile(
      in,
      uniqueFilename,
      sessionUser1
    )
    assert(response.getStatusInfo.getStatusCode == 200)
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResources(sessionUser1, getKeywordsArray("common"))
    assert(DashboardClickableFileEntryList.results.length == 2)
  }

  it should "handle multiple keywords correctly" in {
    projectResource.createProject(sessionUser1, "test project1")
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    val in = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    val filename = "test.csv"
    val response = fileResource.uploadFile(
      in,
      filename,
      sessionUser1
    )
    assert(response.getStatusInfo.getStatusCode == 200)

    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResources(sessionUser1, getKeywordsArray("test", "project1"))
    assert(
      DashboardClickableFileEntryList.results.length == 1
    ) // should only return the project
  }

  it should "filter results by different resourceType" in {
    // create different types of resources
    // 3 projects, 2 file, and 1 workflow,
    projectResource.createProject(sessionUser1, "test project1")
    projectResource.createProject(sessionUser1, "test project2")
    projectResource.createProject(sessionUser1, "test project3")
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    val fileResource = new UserFileResource()
    val in = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    val filename = "test.csv"
    var response = fileResource.uploadFile(
      in,
      filename,
      sessionUser1
    )
    assert(response.getStatusInfo.getStatusCode == 200)
    response = fileResource.uploadFile(
      in,
      "test.js",
      sessionUser1
    )
    assert(response.getStatusInfo.getStatusCode == 200)
    // search resources with all resourceType
    var DashboardClickableFileEntryList =
      dashboardResource.searchAllResources(sessionUser1, getKeywordsArray("test"))
    assert(DashboardClickableFileEntryList.results.length == 6)

    // filter resources by workflow
    DashboardClickableFileEntryList =
      dashboardResource.searchAllResources(sessionUser1, getKeywordsArray("test"), "workflow")
    assert(DashboardClickableFileEntryList.results.length == 1)

    // filter resources by project
    DashboardClickableFileEntryList =
      dashboardResource.searchAllResources(sessionUser1, getKeywordsArray("test"), "project")
    assert(DashboardClickableFileEntryList.results.length == 3)

    // filter resources by file
    DashboardClickableFileEntryList =
      dashboardResource.searchAllResources(sessionUser1, getKeywordsArray("test"), "file")
    assert(DashboardClickableFileEntryList.results.length == 2)

  }

  it should "throw an BadRequestException for invalid resourceType" in {
    assertThrows[BadRequestException] {
      dashboardResource.searchAllResources(
        sessionUser1,
        getKeywordsArray("test"),
        "invalid-resource-type"
      )
    }
  }
  it should "return resources that match any of all provided keywords" in {
    // This test is designed to verify that the searchAllResources function correctly
    // returns resources that match all of the provided keywords

    // Create different types of resources, a project, a workflow, and a file
    projectResource.createProject(sessionUser1, "test project")
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    val in = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    val filename = "unique.csv"
    val response = fileResource.uploadFile(
      in,
      filename,
      sessionUser1
    )
    assert(response.getStatusInfo.getStatusCode == 200)

    // Perform search with multiple keywords
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResources(sessionUser1, getKeywordsArray("test", "project"))

    // Assert that the search results include resources that match any of the provided keywords
    assert(DashboardClickableFileEntryList.results.length == 1)
  }

  it should "not return resources that belong to a different user" in {
    // This test is designed to verify that the searchAllResources function does not return resources that belong to a different user

    // Create a project for a different user (sessionUser2)
    projectResource.createProject(sessionUser2, "test project2")

    // Perform search for resources using sessionUser1
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResources(sessionUser1, getKeywordsArray("test"))

    // Assert that the search results do not include the project that belongs to the different user
    // Assuming that DashboardClickableFileEntryList is a list of resources where each resource has a `user` property
    assert(DashboardClickableFileEntryList.results.length == 0)
  }

  it should "handle reserved characters in the keywords in searchAllResources" in {
    // testWorkflow1: {name: test_name, description: test_description, content: "key pair"}
    // search "key+-pair" or "key@pair" or "key+" or "+key" should return testWorkflow1
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)

    // search with reserved characters in keywords
    var DashboardClickableFileEntryList = dashboardResource.searchAllResources(
      sessionUser1,
      getKeywordsArray(keywordInWorkflow1Content + "+-@()<>~*\"" + keywordInWorkflow1Content)
    )
    assert(DashboardClickableFileEntryList.results.length == 1)

    DashboardClickableFileEntryList = dashboardResource.searchAllResources(
      sessionUser1,
      getKeywordsArray(keywordInWorkflow1Content + "@" + keywordInWorkflow1Content)
    )
    assert(DashboardClickableFileEntryList.results.size == 1)

    DashboardClickableFileEntryList = dashboardResource.searchAllResources(
      sessionUser1,
      getKeywordsArray(keywordInWorkflow1Content + "+-@()<>~*\"")
    )
    assert(DashboardClickableFileEntryList.results.size == 1)

    DashboardClickableFileEntryList = dashboardResource.searchAllResources(
      sessionUser1,
      getKeywordsArray("+-@()<>~*\"" + keywordInWorkflow1Content)
    )
    assert(DashboardClickableFileEntryList.results.size == 1)

  }

  it should "paginate results correctly" in {
    // This test is designed to verify that the pagination works correctly

    // Create 1 workflow, 10 projects, 10 files
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    for (i <- 1 to 10) {
      projectResource.createProject(sessionUser1, s"test project $i")
      val in = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
      val filename = s"test_file$i.csv"
      val response = fileResource.uploadFile(in, filename, sessionUser1)
      assert(response.getStatusInfo.getStatusCode == 200)
    }

    // Request the first page of results (page size is 10)
    val firstPage = dashboardResource.searchAllResources(sessionUser1, count = 10)

    // Assert that the first page has 10 results
    assert(firstPage.results.length == 10)
    assert(firstPage.more) // Assert that there are more results to be fetched

    // Request the second page of results
    val secondPage = dashboardResource.searchAllResources(sessionUser1, count = 10, offset = 10)

    // Assert that the second page has 10 results
    assert(secondPage.results.length == 10)
    assert(secondPage.more) // Assert that there are more results to be fetched

    // Request the third page of results
    val thirdPage = dashboardResource.searchAllResources(sessionUser1, count = 10, offset = 20)

    // Assert that the third page has 5 results (since we only have 25 resources)
    assert(thirdPage.results.length == 1)
    assert(!thirdPage.more) // Assert that there are no more results to be fetched

    // Assert that the results are unique across all pages
    val allResults = firstPage.results ++ secondPage.results ++ thirdPage.results
    assert(allResults.distinct.length == allResults.length)
  }

  it should "order workflow by name correctly" in {
    // Create several resources with different names
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow2, sessionUser1)

    // Retrieve resources ordered by name in ascending order
    var resources =
      dashboardResource.searchAllResources(
        sessionUser1,
        resourceType = "workflow",
        orderBy = "NameAsc"
      )

    // Check the order of the results
    assert(resources.results(0).workflow.workflow.getName() == "test_workflow1")
    assert(resources.results(1).workflow.workflow.getName() == "test_workflow2")
    assert(resources.results(2).workflow.workflow.getName() == "test_workflow3")

    resources = dashboardResource.searchAllResources(
      sessionUser1,
      resourceType = "workflow",
      orderBy = "NameDesc"
    )
    // Check the order of the results
    assert(resources.results(0).workflow.workflow.getName() == "test_workflow3")
    assert(resources.results(1).workflow.workflow.getName() == "test_workflow2")
    assert(resources.results(2).workflow.workflow.getName() == "test_workflow1")
  }

  it should "order workflow by creation time in descending order correctly" in {
    // Create several resources with different creation times
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    Thread.sleep(1000)
    workflowResource.persistWorkflow(testWorkflow2, sessionUser1)
    Thread.sleep(1000)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)

    // Retrieve resources ordered by creation time in descending order
    var resources =
      dashboardResource.searchAllResources(
        sessionUser1,
        resourceType = "workflow",
        orderBy = "CreateTimeDesc"
      )

    // Check the order of the results
    assert(resources.results(0).workflow.workflow.getName() == "test_workflow3")
    assert(resources.results(1).workflow.workflow.getName() == "test_workflow2")
    assert(resources.results(2).workflow.workflow.getName() == "test_workflow1")
  }

  it should "order project by name in ascending order correctly" in {
    // Create several resources with different names
    projectResource.createProject(sessionUser1, "test project C")
    projectResource.createProject(sessionUser1, "test project A")
    projectResource.createProject(sessionUser1, "test project B")

    // Retrieve resources ordered by name in ascending order
    val resources =
      dashboardResource.searchAllResources(
        sessionUser1,
        resourceType = "project",
        orderBy = "NameAsc"
      )

    // Check the order of the results
    assert(resources.results(0).project.getName() == "test project A")
    assert(resources.results(1).project.getName() == "test project B")
    assert(resources.results(2).project.getName() == "test project C")
  }

  it should "order project by name in descending order correctly" in {
    // Create several resources with different names
    projectResource.createProject(sessionUser1, "test project C")
    projectResource.createProject(sessionUser1, "test project A")
    projectResource.createProject(sessionUser1, "test project B")

    // Retrieve resources ordered by name in descending order
    val resources =
      dashboardResource.searchAllResources(
        sessionUser1,
        resourceType = "project",
        orderBy = "NameDesc"
      )

    // Check the order of the results
    assert(resources.results(0).project.getName() == "test project C")
    assert(resources.results(1).project.getName() == "test project B")
    assert(resources.results(2).project.getName() == "test project A")
  }

  it should "order project by creation time in descending order correctly" in {
    // Create several resources with different creation times
    projectResource.createProject(sessionUser1, "test project A")
    Thread.sleep(1000)
    projectResource.createProject(sessionUser1, "test project B")
    Thread.sleep(1000)
    projectResource.createProject(sessionUser1, "test project C")

    // Retrieve resources ordered by creation time in descending order
    val resources =
      dashboardResource.searchAllResources(
        sessionUser1,
        resourceType = "project",
        orderBy = "CreateTimeDesc"
      )

    // Check the order of the results
    assert(resources.results(0).project.getName() == "test project C")
    assert(resources.results(1).project.getName() == "test project B")
    assert(resources.results(2).project.getName() == "test project A")
  }

  it should "throw a BadRequestException when given an unknown orderBy value" in {
    // Attempt to retrieve resources with an invalid orderBy value
    assertThrows[BadRequestException] {
      dashboardResource.searchAllResources(sessionUser1, orderBy = "InvalidOrderBy")
    }
  }

  it should "order file by name in ascending order correctly" in {
    // Create several resources with different names
    val inA = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    fileResource.uploadFile(inA, "test file A", sessionUser1)
    Thread.sleep(1000)
    val inB = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    fileResource.uploadFile(inB, "test file B", sessionUser1)
    Thread.sleep(1000)
    val inC = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    fileResource.uploadFile(inC, "test file C", sessionUser1)

    // Retrieve resources ordered by name in ascending order
    var resources =
      dashboardResource.searchAllResources(sessionUser1, resourceType = "file", orderBy = "NameAsc")

    // Check the order of the results
    assert(resources.results(0).file.file.getName() == "test file A")
    assert(resources.results(1).file.file.getName() == "test file B")
    assert(resources.results(2).file.file.getName() == "test file C")

    // Retrieve resources ordered by name in descending order
    resources = dashboardResource.searchAllResources(
      sessionUser1,
      resourceType = "file",
      orderBy = "NameDesc"
    )
    // Check the order of the results
    assert(resources.results(2).file.file.getName() == "test file A")
    assert(resources.results(1).file.file.getName() == "test file B")
    assert(resources.results(0).file.file.getName() == "test file C")
  }

  it should "order file by creation time in descending order correctly" in {
    // Create several resources with different names
    val inA = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    fileResource.uploadFile(inA, "test file B", sessionUser1)
    Thread.sleep(1000)
    val inB = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    fileResource.uploadFile(inB, "test file A", sessionUser1)
    Thread.sleep(1000)
    val inC = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    fileResource.uploadFile(inC, "test file C", sessionUser1)

    // Retrieve resources ordered by creation time in descending order
    val resources =
      dashboardResource.searchAllResources(
        sessionUser1,
        resourceType = "file",
        orderBy = "CreateTimeDesc"
      )

    assert(resources.results(0).file.file.getName() == "test file C")
    assert(resources.results(1).file.file.getName() == "test file A")
    assert(resources.results(2).file.file.getName() == "test file B")
  }

  it should "order all resource types by creation_time in descending order correctly" in {
    // Create resources
    val inA = org.apache.commons.io.IOUtils.toInputStream("", "UTF-8")
    fileResource.uploadFile(inA, "test file C", sessionUser1)
    Thread.sleep(1000)
    projectResource.createProject(sessionUser1, "test project B")
    Thread.sleep(1000)
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    Thread.sleep(1000)

    // Retrieve resources ordered by name in descending order
    val resources = dashboardResource.searchAllResources(sessionUser1, orderBy = "CreateTimeDesc")
    assert(resources.results.length == 3)
    // Check the order of the results
    assert(resources.results(2).file.file.getName == "test file C")
    assert(resources.results(1).project.getName == "test project B")
    assert(resources.results(0).workflow.workflow.getName() == "test_workflow1")
  }

}
