package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowDao, WorkflowVersionDao}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{User, Workflow, WorkflowVersion}
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

  private val workflowDao: WorkflowDao = new WorkflowDao(getDSLContext.configuration())
  private val workflowVersionDao: WorkflowVersionDao = new WorkflowVersionDao(
    getDSLContext.configuration()
  )
  private val userDao: UserDao = new UserDao(getDSLContext.configuration())

  // Helper method to create a test workflow
  private def createTestWorkflow(name: String, content: String): Workflow = {
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
    userDao.insert(testUser)
  }

  override protected def afterEach(): Unit = {
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

  // Test case to verify that retrieveWorkflowVersion correctly applies patches in the right order
  "retrieveWorkflowVersion" should "correctly apply patches in chronological order" in {
    // Create a workflow with initial content
    val initialContent = """{"operators": {"op1": {"id": "op1", "value": 1}}}"""
    val workflow = createTestWorkflow("Test Workflow", initialContent)
    workflowDao.insert(workflow)
    val wid = workflow.getWid

    // Create a series of versions with different patches
    // Version 1: Initial version (automatically created by the system)
    WorkflowVersionResource.insertNewVersion(wid)
    Thread.sleep(100) // ensure different timestamps

    // Version 2: Change op1 value from 1 to 2
    val patchContent2 = """[{"op":"replace","path":"/operators/op1/value","value":2}]"""
    createWorkflowVersion(wid, patchContent2)
    Thread.sleep(100)

    // Version 3: Change op1 value from 2 to 3
    val patchContent3 = """[{"op":"replace","path":"/operators/op1/value","value":3}]"""
    createWorkflowVersion(wid, patchContent3)
    Thread.sleep(100)

    // Version 4: Change op1 value from 3 to 4
    val patchContent4 = """[{"op":"replace","path":"/operators/op1/value","value":4}]"""
    val v4 = createWorkflowVersion(wid, patchContent4)
    val vid = v4.getVid

    // Retrieve versions to get the original workflow
    val restoredWorkflow = workflowVersionResource.retrieveWorkflowVersion(wid, vid, sessionUser)

    // Parse the JSON content to verify the value
    val jsonNode = objectMapper.readTree(restoredWorkflow.getContent)
    val op1Value = jsonNode.path("operators").path("op1").path("value").asInt()

    // The value should be 1 because we're applying patches from vid to the latest version
    // Each patch reverts one change, going backwards from 4 to 3 to 2 to 1
    assert(op1Value === 1)
  }

  // Test case to verify that retrieveWorkflowVersion with the latest vid returns the current workflow
  "retrieveWorkflowVersion" should "return the current workflow when using the latest vid" in {
    // Create a workflow with initial content
    val initialContent = """{"operators": {"op1": {"id": "op1", "value": 1}}}"""
    val workflow = createTestWorkflow("Test Workflow", initialContent)
    workflowDao.insert(workflow)
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
