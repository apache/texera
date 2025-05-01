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

import scala.jdk.CollectionConverters._

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

  private val sessionUser: SessionUser = {
    new SessionUser(testUser)
  }

  private val workflowVersionResource: WorkflowVersionResource = {
    new WorkflowVersionResource()
  }

  // Initialize DAOs after database setup instead of during class initialization
  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var userDao: UserDao = _
  private var workflowOfUserDao: WorkflowOfUserDao = _

  // Helper method to create a test workflow
  private def createTestWorkflow(name: String, content: String): Workflow = {
    val workflow = createTestWorkflowOnly(name, content)
    workflowDao.insert(workflow)

    // Set the workflow owner to the test user
    val workflowOfUser = new WorkflowOfUser
    workflowOfUser.setWid(workflow.getWid)
    workflowOfUser.setUid(testUser.getUid)
    workflowOfUserDao.insert(workflowOfUser)

    workflow
  }

  // Helper method to create a workflow without inserting into DB
  private def createTestWorkflowOnly(name: String, content: String): Workflow = {
    val workflow = new Workflow()
    workflow.setName(name)
    workflow.setDescription("Test description")
    workflow.setContent(content)
    workflow
  }

  // Helper method to create a version of a workflow
  private def createWorkflowVersion(wid: Integer, content: String): WorkflowVersion = {
    val version = new WorkflowVersion()
    version.setWid(wid)
    version.setContent(content)
    workflowVersionDao.insert(version)
    version
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    // Initialize the DAOs after the database is set up
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
    userDao = new UserDao(getDSLContext.configuration())
    workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())
    userDao.insert(testUser)
  }

  override protected def afterEach(): Unit = {
    // Clean up all workflow of user entries
    val allWorkflowOfUsers = workflowOfUserDao.findAll().asScala
    allWorkflowOfUsers.foreach(entry => workflowOfUserDao.delete(entry))

    // Clean up all workflow versions
    val allVersions = workflowVersionDao.findAll().asScala
    allVersions.foreach(version => workflowVersionDao.delete(version))

    // Clean up all workflows
    val allWorkflows = workflowDao.findAll().asScala
    allWorkflows.foreach(workflow => workflowDao.delete(workflow))
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  // Test case to verify that retrieveWorkflowVersion correctly applies patches in chronological order
  "retrieveWorkflowVersion" should "correctly apply patches in chronological order" in {
    // Create a workflow with initial content
    val initialContent = """{"operators": {"op1": {"id": "op1", "value": 1}}}"""
    val workflow = createTestWorkflow("Test Workflow", initialContent)
    val wid = workflow.getWid

    // Create a series of versions with different patches (at least 10)
    // Version 1: Initial version (automatically created by the system)
    WorkflowVersionResource.insertNewVersion(wid)
    Thread.sleep(50) // ensure different timestamps

    // Create 10 versions that change the value in sequence from 1 to 11
    var lastVersion: WorkflowVersion = null
    for (i <- 2 to 11) {
      val patchContent = s"""[{"op":"replace","path":"/operators/op1/value","value":$i}]"""
      lastVersion = createWorkflowVersion(wid, patchContent)
      Thread.sleep(50) // ensure different timestamps
    }

    val vid = lastVersion.getVid

    // Retrieve versions to get the original workflow
    val restoredWorkflow = workflowVersionResource.retrieveWorkflowVersion(wid, vid, sessionUser)

    // Parse the JSON content to verify the value
    val jsonNode = objectMapper.readTree(restoredWorkflow.getContent)
    val op1Value = jsonNode.path("operators").path("op1").path("value").asInt()

    // The value should be 1 because we're applying patches from vid to the latest version
    // Each patch reverts one change, going backwards from 11 to 10 to 9... to 1
    assert(op1Value === 1)
  }

  // Additional test case with 10 versions applying different types of changes
  "retrieveWorkflowVersion" should "correctly handle a mix of different patch operations" in {
    // Create a workflow with initial content
    val initialContent = """{"operators": {"op1": {"id": "op1", "value": 1, "config": {}}}}"""
    val workflow = createTestWorkflow("Test Workflow", initialContent)
    val wid = workflow.getWid

    // Create the initial version
    WorkflowVersionResource.insertNewVersion(wid)
    Thread.sleep(50)

    // Version 2: Change value from 1 to 2
    createWorkflowVersion(wid, """[{"op":"replace","path":"/operators/op1/value","value":2}]""")
    Thread.sleep(50)

    // Version 3: Add a new property
    createWorkflowVersion(
      wid,
      """[{"op":"add","path":"/operators/op1/name","value":"Operator 1"}]"""
    )
    Thread.sleep(50)

    // Version 4: Add a nested property
    createWorkflowVersion(wid, """[{"op":"add","path":"/operators/op1/config/size","value":10}]""")
    Thread.sleep(50)

    // Version 5: Change the nested property
    createWorkflowVersion(
      wid,
      """[{"op":"replace","path":"/operators/op1/config/size","value":20}]"""
    )
    Thread.sleep(50)

    // Version 6: Add another operator
    createWorkflowVersion(
      wid,
      """[{"op":"add","path":"/operators/op2","value":{"id":"op2","value":5}}]"""
    )
    Thread.sleep(50)

    // Version 7: Remove a property
    createWorkflowVersion(wid, """[{"op":"remove","path":"/operators/op1/name"}]""")
    Thread.sleep(50)

    // Version 8: Replace the entire config object
    createWorkflowVersion(
      wid,
      """[{"op":"replace","path":"/operators/op1/config","value":{"color":"blue"}}]"""
    )
    Thread.sleep(50)

    // Version 9: Update the second operator
    createWorkflowVersion(wid, """[{"op":"replace","path":"/operators/op2/value","value":10}]""")
    Thread.sleep(50)

    // Version 10: Add a new top-level property
    val v10 = createWorkflowVersion(
      wid,
      """[{"op":"add","path":"/links","value":[{"source":"op1","target":"op2"}]}]"""
    )
    val vid = v10.getVid

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
    // Create a workflow with initial content
    val initialContent = """{"operators": {"op1": {"id": "op1", "value": 1}}}"""
    val workflow = createTestWorkflow("Test Workflow", initialContent)
    val wid = workflow.getWid

    // Update the workflow content
    val updatedContent = """{"operators": {"op1": {"id": "op1", "value": 99}}}"""
    workflow.setContent(updatedContent)
    workflowDao.update(workflow)

    // Create a version (this happens automatically in the real system)
    val version = WorkflowVersionResource.insertNewVersion(wid)
    val vid = version.getVid

    // Retrieve the latest version
    val retrievedWorkflow = workflowVersionResource.retrieveWorkflowVersion(wid, vid, sessionUser)

    // Verify the content is the same as the current workflow
    assert(retrievedWorkflow.getContent === updatedContent)
  }
}
