package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowVersionDao,
  WorkflowOfUserDao
}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowVersion,
  WorkflowOfUser
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import edu.uci.ics.amber.engine.common.Utils.objectMapper

class WorkflowVersionResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val testUser: User = {
    val user = new User
    user.setUid(Integer.valueOf(1))
    user.setName("test_user")
    user.setRole(UserRoleEnum.ADMIN)
    user.setPassword("123")
    user
  }

  private val sessionUser: SessionUser = new SessionUser(testUser)

  private val workflowVersionResource: WorkflowVersionResource = new WorkflowVersionResource()

  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var userDao: UserDao = _
  private var workflowOfUserDao: WorkflowOfUserDao = _

  // Test workflow for the first test
  private var testWorkflow1: Workflow = _
  private var testWorkflow1Versions: List[WorkflowVersion] = List()
  private var lastVersionId1: Integer = _

  // Test workflow for the second test
  private var testWorkflow2: Workflow = _
  private var testWorkflow2Versions: List[WorkflowVersion] = List()
  private var lastVersionId2: Integer = _

  // Test workflow for the third test
  private var testWorkflow3: Workflow = _
  private var testWorkflow3Version: WorkflowVersion = _

  override protected def beforeAll(): Unit = {
    // Initialize database and create DAOs
    initializeDBAndReplaceDSLContext()
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
    userDao = new UserDao(getDSLContext.configuration())
    workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())

    // Insert test user
    userDao.insert(testUser)

    // Prepare Test 1: Workflow with sequential versions
    testWorkflow1 = createTestWorkflowOnly(
      "Test Workflow 1",
      """{"operators": {"op1": {"id": "op1", "value": 1}}}"""
    )
    workflowDao.insert(testWorkflow1)
    setupWorkflowOwnership(testWorkflow1.getWid)

    // Create initial version
    val initialVersion1 = WorkflowVersionResource.insertNewVersion(testWorkflow1.getWid)
    testWorkflow1Versions = testWorkflow1Versions :+ initialVersion1
    Thread.sleep(50)

    // Create 10 versions with incrementing values
    for (i <- 2 to 11) {
      val patchContent = s"""[{"op":"replace","path":"/operators/op1/value","value":$i}]"""
      val version = createVersionForWorkflow(testWorkflow1.getWid, patchContent)
      testWorkflow1Versions = testWorkflow1Versions :+ version
      Thread.sleep(50)
    }
    lastVersionId1 = testWorkflow1Versions.last.getVid

    // Prepare Test 2: Workflow with mixed operations
    testWorkflow2 = createTestWorkflowOnly(
      "Test Workflow 2",
      """{"operators": {"op1": {"id": "op1", "value": 1, "config": {}}}}"""
    )
    workflowDao.insert(testWorkflow2)
    setupWorkflowOwnership(testWorkflow2.getWid)

    // Create initial version
    val initialVersion2 = WorkflowVersionResource.insertNewVersion(testWorkflow2.getWid)
    testWorkflow2Versions = testWorkflow2Versions :+ initialVersion2
    Thread.sleep(50)

    // Add different types of changes
    val changes = List(
      """[{"op":"replace","path":"/operators/op1/value","value":2}]""",
      """[{"op":"add","path":"/operators/op1/name","value":"Operator 1"}]""",
      """[{"op":"add","path":"/operators/op1/config/size","value":10}]""",
      """[{"op":"replace","path":"/operators/op1/config/size","value":20}]""",
      """[{"op":"add","path":"/operators/op2","value":{"id":"op2","value":5}}]""",
      """[{"op":"remove","path":"/operators/op1/name"}]""",
      """[{"op":"replace","path":"/operators/op1/config","value":{"color":"blue"}}]""",
      """[{"op":"replace","path":"/operators/op2/value","value":10}]""",
      """[{"op":"add","path":"/links","value":[{"source":"op1","target":"op2"}]}]"""
    )

    for (change <- changes) {
      val version = createVersionForWorkflow(testWorkflow2.getWid, change)
      testWorkflow2Versions = testWorkflow2Versions :+ version
      Thread.sleep(50)
    }
    lastVersionId2 = testWorkflow2Versions.last.getVid

    // Prepare Test 3: Latest version test
    val initialContent = """{"operators": {"op1": {"id": "op1", "value": 1}}}"""
    val updatedContent = """{"operators": {"op1": {"id": "op1", "value": 99}}}"""
    testWorkflow3 = createTestWorkflowOnly("Test Workflow 3", initialContent)
    workflowDao.insert(testWorkflow3)
    setupWorkflowOwnership(testWorkflow3.getWid)

    // Update the workflow content
    testWorkflow3.setContent(updatedContent)
    workflowDao.update(testWorkflow3)

    // Create a version
    testWorkflow3Version = WorkflowVersionResource.insertNewVersion(testWorkflow3.getWid)
  }

  private def setupWorkflowOwnership(wid: Integer): Unit = {
    val workflowOfUser = new WorkflowOfUser
    workflowOfUser.setWid(wid)
    workflowOfUser.setUid(testUser.getUid)
    workflowOfUserDao.insert(workflowOfUser)
  }

  private def createTestWorkflowOnly(name: String, content: String): Workflow = {
    val workflow = new Workflow()
    workflow.setName(name)
    workflow.setDescription("Test description")
    workflow.setContent(content)
    workflow
  }

  private def createVersionForWorkflow(wid: Integer, content: String): WorkflowVersion = {
    val version = new WorkflowVersion()
    version.setWid(wid)
    version.setContent(content)
    workflowVersionDao.insert(version)
    version
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  // Test case to verify that retrieveWorkflowVersion correctly applies patches in chronological order
  "retrieveWorkflowVersion" should "correctly apply patches in chronological order" in {
    // Use the pre-created objects from beforeAll
    val wid = testWorkflow1.getWid
    val vid = lastVersionId1

    // Retrieve versions to get the original workflow
    val restoredWorkflow = workflowVersionResource.retrieveWorkflowVersion(wid, vid, sessionUser)

    // Parse the JSON content to verify the value
    val jsonNode = objectMapper.readTree(restoredWorkflow.getContent)
    val op1Value = jsonNode.path("operators").path("op1").path("value").asInt()

    // The value should be 1 because we're applying patches from vid to the latest version
    // Each patch reverts one change, going backwards from 11 to 10 to 9... to 1
    assert(op1Value === 1)
  }

  // Additional test case with different types of changes
  "retrieveWorkflowVersion" should "correctly handle a mix of different patch operations" in {
    // Use the pre-created objects from beforeAll
    val wid = testWorkflow2.getWid
    val vid = lastVersionId2

    // Retrieve the original workflow state
    val restoredWorkflow = workflowVersionResource.retrieveWorkflowVersion(wid, vid, sessionUser)

    // Parse the JSON content to verify
    val jsonNode = objectMapper.readTree(restoredWorkflow.getContent)

    // Verify content matches the initial state after all patches are applied
    assert(jsonNode.path("operators").path("op1").path("value").asInt() === 1)
    assert(!jsonNode.path("operators").path("op1").has("name"))
    assert(!jsonNode.path("operators").has("op2"))
    assert(!jsonNode.has("links"))
    assert(jsonNode.path("operators").path("op1").path("config").isObject)
    assert(jsonNode.path("operators").path("op1").path("config").isEmpty)
  }

  // Test case to verify that retrieveWorkflowVersion with the latest vid returns the current workflow
  "retrieveWorkflowVersion" should "return the current workflow when using the latest vid" in {
    // Use the pre-created objects from beforeAll
    val wid = testWorkflow3.getWid
    val vid = testWorkflow3Version.getVid

    // Retrieve the latest version
    val retrievedWorkflow = workflowVersionResource.retrieveWorkflowVersion(wid, vid, sessionUser)

    // Verify the content is the same as the current workflow
    assert(retrievedWorkflow.getContent === testWorkflow3.getContent)
  }
}
