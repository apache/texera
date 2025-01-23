package edu.uci.ics.texera.web.resource.dashboard.file

import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.web.auth.SessionUser
import edu.uci.ics.texera.dao.jooq.generated.Tables.{USER, WORKFLOW, WORKFLOW_OF_PROJECT}
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRole
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{Project, User, Workflow}
import edu.uci.ics.texera.web.resource.dashboard.DashboardResource.SearchQueryParams
import edu.uci.ics.texera.web.resource.dashboard.user.project.ProjectResource
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowResource.{
  DashboardWorkflow,
  WorkflowIDs
}
import edu.uci.ics.texera.web.resource.dashboard.{DashboardResource, FulltextSearchQueryUtils}
import org.jooq.Condition
import org.jooq.impl.DSL.noCondition
import org.jooq.types.UInteger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.sql.Timestamp
import java.text.{ParseException, SimpleDateFormat}
import java.util
import java.util.Collections
import java.util.concurrent.TimeUnit

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
      workflowResource.deleteWorkflow(
        WorkflowIDs(List(workflow.workflow.getWid), None),
        sessionUser1
      )
    )

    workflows = workflowResource.retrieveWorkflowsBySessionUser(sessionUser2)
    workflows.foreach(workflow =>
      workflowResource.deleteWorkflow(
        WorkflowIDs(List(workflow.workflow.getWid), None),
        sessionUser2
      )
    )

    // delete all projects in the database
    var projects = projectResource.getProjectList(sessionUser1)
    projects.forEach(project => projectResource.deleteProject(project.pid))

    projects = projectResource.getProjectList(sessionUser2)
    projects.forEach(project => projectResource.deleteProject(project.pid))

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

  it should "search for keywords containing special characters" in {
    // testWorkflow1: {name: test_name, description: test_description, content: "key@gmail.com"}
    // search "key@gmail.com" should return testWorkflow1
    workflowResource.persistWorkflow(testWorkflowWithSpecialCharacters, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow3, sessionUser1)
    val DashboardWorkflowEntryList =
      dashboardResource
        .searchAllResourcesCall(
          sessionUser1,
          SearchQueryParams(getKeywordsArray(exampleEmailAddress))
        )
        .results
    assert(DashboardWorkflowEntryList.size == 1)
    assertSameWorkflow(
      testWorkflowWithSpecialCharacters,
      DashboardWorkflowEntryList.head.workflow.get
    )
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

  it should "return a proper condition for a single projectId" in {
    val projectIdList = new java.util.ArrayList[UInteger](util.Arrays.asList(UInteger.valueOf(1)))
    val projectFilter: Condition =
      FulltextSearchQueryUtils.getContainsFilter(projectIdList, WORKFLOW_OF_PROJECT.PID)
    assert(projectFilter.toString == WORKFLOW_OF_PROJECT.PID.eq(UInteger.valueOf(1)).toString)
  }

  it should "return a proper condition for multiple projectIds" in {
    val projectIdList = new java.util.ArrayList[UInteger](
      util.Arrays.asList(UInteger.valueOf(1), UInteger.valueOf(2))
    )
    val projectFilter: Condition =
      FulltextSearchQueryUtils.getContainsFilter(projectIdList, WORKFLOW_OF_PROJECT.PID)
    assert(
      projectFilter.toString == WORKFLOW_OF_PROJECT.PID
        .eq(UInteger.valueOf(1))
        .or(WORKFLOW_OF_PROJECT.PID.eq(UInteger.valueOf(2)))
        .toString
    )
  }

  it should "return a proper condition for a single workflowID" in {
    val workflowIdList = new java.util.ArrayList[UInteger](util.Arrays.asList(UInteger.valueOf(1)))
    val workflowIdFilter: Condition =
      FulltextSearchQueryUtils.getContainsFilter(workflowIdList, WORKFLOW.WID)
    assert(workflowIdFilter.toString == WORKFLOW.WID.eq(UInteger.valueOf(1)).toString)
  }

  it should "return a proper condition for multiple workflowIDs" in {
    val workflowIdList = new java.util.ArrayList[UInteger](
      util.Arrays.asList(UInteger.valueOf(1), UInteger.valueOf(2))
    )
    val workflowIdFilter: Condition =
      FulltextSearchQueryUtils.getContainsFilter(workflowIdList, WORKFLOW.WID)
    assert(
      workflowIdFilter.toString == WORKFLOW.WID
        .eq(UInteger.valueOf(1))
        .or(WORKFLOW.WID.eq(UInteger.valueOf(2)))
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

  "/search API" should "be able to search for resources in different tables" in {

    // create different types of resources, project, workflow, and file
    projectResource.createProject(sessionUser1, "test project1")
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    // search
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(getKeywordsArray("test"))
      )
    assert(DashboardClickableFileEntryList.results.length == 2)

  }

  it should "return all resources when no keyword provided" in {
    projectResource.createProject(sessionUser1, "test project1")
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(getKeywordsArray(""))
      )
    assert(DashboardClickableFileEntryList.results.length == 2)
  }

  it should "return multiple matching resources from a single resource type" in {
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    projectResource.createProject(sessionUser1, "common project1")
    projectResource.createProject(sessionUser1, "common project2")
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(getKeywordsArray("common"))
      )
    assert(DashboardClickableFileEntryList.results.length == 2)
  }

  it should "handle multiple keywords correctly" in {
    projectResource.createProject(sessionUser1, "test project1")
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(getKeywordsArray("test", "project1"))
      )
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
    // search resources with all resourceType
    var DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(getKeywordsArray("test"))
      )
    assert(DashboardClickableFileEntryList.results.length == 4)

    // filter resources by workflow
    DashboardClickableFileEntryList = dashboardResource.searchAllResourcesCall(
      sessionUser1,
      SearchQueryParams(resourceType = "workflow", keywords = getKeywordsArray("test"))
    )
    assert(DashboardClickableFileEntryList.results.length == 1)

    // filter resources by project
    DashboardClickableFileEntryList = dashboardResource.searchAllResourcesCall(
      sessionUser1,
      SearchQueryParams(resourceType = "project", keywords = getKeywordsArray("test"))
    )
    assert(DashboardClickableFileEntryList.results.length == 3)
  }

  it should "return resources that match any of all provided keywords" in {
    // This test is designed to verify that the searchAllResources function correctly
    // returns resources that match all of the provided keywords

    // Create different types of resources, a project, a workflow, and a file
    projectResource.createProject(sessionUser1, "test project")
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    // Perform search with multiple keywords
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(keywords = getKeywordsArray("test", "project"))
      )

    // Assert that the search results include resources that match any of the provided keywords
    assert(DashboardClickableFileEntryList.results.length == 1)
  }

  it should "not return resources that belong to a different user" in {
    // This test is designed to verify that the searchAllResources function does not return resources that belong to a different user

    // Create a project for a different user (sessionUser2)
    projectResource.createProject(sessionUser2, "test project2")

    // Perform search for resources using sessionUser1
    val DashboardClickableFileEntryList =
      dashboardResource.searchAllResourcesCall(
        sessionUser1,
        SearchQueryParams(keywords = getKeywordsArray("test"))
      )

    // Assert that the search results do not include the project that belongs to the different user
    // Assuming that DashboardClickableFileEntryList is a list of resources where each resource has a `user` property
    assert(DashboardClickableFileEntryList.results.isEmpty)
  }

  it should "paginate results correctly" in {
    // This test is designed to verify that the pagination works correctly

    // Create 1 workflow, 10 projects
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    for (i <- 1 to 10) {
      projectResource.createProject(sessionUser1, s"test project $i")
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

}
