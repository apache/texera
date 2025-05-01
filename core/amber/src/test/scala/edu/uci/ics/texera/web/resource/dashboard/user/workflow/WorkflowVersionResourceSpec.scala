package edu.uci.ics.texera.web.resource.dashboard.user.workflow

import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{WorkflowDao, WorkflowVersionDao}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{Workflow, WorkflowVersion}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import edu.uci.ics.amber.engine.common.Utils.objectMapper

class WorkflowVersionResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
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

  override protected def beforeEach(): Unit = {
    // Initialize database and create DAOs before each test
    initializeDBAndReplaceDSLContext()
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
  }

  override protected def afterEach(): Unit = {
    shutdownDB()
  }

  private def createTestWorkflow(name: String, content: String): Workflow = {
    val workflow = new Workflow()
    workflow.setName(name)
    workflow.setDescription("Test description")
    workflow.setContent(content)
    workflowDao.insert(workflow)
    workflow
  }

  private def createVersionForWorkflow(wid: Integer, content: String): WorkflowVersion = {
    val version = new WorkflowVersion()
    version.setWid(wid)
    version.setContent(content)
    workflowVersionDao.insert(version)
    version
  }

  // Test case to verify that fetchWorkflowVersion correctly applies patches in chronological order
  "fetchWorkflowVersion" should "correctly apply patches in chronological order" in {
    // Create a workflow with initial content
    val initialContent = """{"operators": {"op1": {"id": "op1", "value": 1}}}"""
    val workflow = createTestWorkflow("Test Workflow 1", initialContent)
    val wid = workflow.getWid

    // Create initial version
    val initialVersion = WorkflowVersionResource.insertNewVersion(wid)
    Thread.sleep(50)

    // Create 10 versions with incrementing values
    var lastVersion: WorkflowVersion = null
    for (i <- 2 to 11) {
      val patchContent = s"""[{"op":"replace","path":"/operators/op1/value","value":$i}]"""
      lastVersion = createVersionForWorkflow(wid, patchContent)
      Thread.sleep(50)
    }
    val vid = lastVersion.getVid

    // Retrieve versions to get the original workflow
    val restoredWorkflow = WorkflowVersionResource.fetchWorkflowVersion(wid, vid)

    // Parse the JSON content to verify the value
    val jsonNode = objectMapper.readTree(restoredWorkflow.getContent)
    val op1Value = jsonNode.path("operators").path("op1").path("value").asInt()

    // The value should be 1 because we're applying patches from vid to the latest version
    // Each patch reverts one change, going backwards from 11 to 10 to 9... to 1
    assert(op1Value === 1)
  }

  // Additional test case with different types of changes
  "fetchWorkflowVersion" should "correctly handle a mix of different patch operations" in {
    // Create a workflow with initial content
    val initialContent = """{"operators": {"op1": {"id": "op1", "value": 1, "config": {}}}}"""
    val workflow = createTestWorkflow("Test Workflow 2", initialContent)
    val wid = workflow.getWid

    // Create initial version
    val initialVersion = WorkflowVersionResource.insertNewVersion(wid)
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

    var lastVersion: WorkflowVersion = null
    for (change <- changes) {
      lastVersion = createVersionForWorkflow(wid, change)
      Thread.sleep(50)
    }
    val vid = lastVersion.getVid

    // Retrieve the original workflow state
    val restoredWorkflow = WorkflowVersionResource.fetchWorkflowVersion(wid, vid)

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

  // Test case to verify that fetchWorkflowVersion with the latest vid returns the current workflow
  "fetchWorkflowVersion" should "return the current workflow when using the latest vid" in {
    // Create a workflow with initial content
    val initialContent = """{"operators": {"op1": {"id": "op1", "value": 1}}}"""
    val updatedContent = """{"operators": {"op1": {"id": "op1", "value": 99}}}"""
    val workflow = createTestWorkflow("Test Workflow 3", initialContent)
    val wid = workflow.getWid

    // Update the workflow content
    workflow.setContent(updatedContent)
    workflowDao.update(workflow)

    // Create a version
    val version = WorkflowVersionResource.insertNewVersion(wid)
    val vid = version.getVid

    // Retrieve the latest version
    val retrievedWorkflow = WorkflowVersionResource.fetchWorkflowVersion(wid, vid)

    // Verify the content is the same as the current workflow
    assert(retrievedWorkflow.getContent === updatedContent)
  }
}
