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

  // Test case to verify that fetchWorkflowVersion correctly applies patches
  "fetchWorkflowVersion" should "return the workflow at the specified version" in {
    // Create a workflow with initial content
    val initialContent = """{"operators": {"op1": {"id": "op1", "value": 1}}}"""
    val workflow = createTestWorkflow("Test Workflow 1", initialContent)
    val wid = workflow.getWid

    // Create initial version
    val initialVersion = WorkflowVersionResource.insertNewVersion(wid)
    Thread.sleep(50)

    // Create 10 versions with incrementing values
    var versionMap = Map[Integer, Int]()
    var lastVersion: WorkflowVersion = null
    for (i <- 2 to 11) {
      val patchContent = s"""[{"op":"replace","path":"/operators/op1/value","value":$i}]"""
      lastVersion = createVersionForWorkflow(wid, patchContent)
      versionMap += (lastVersion.getVid -> i)
      Thread.sleep(50)
    }
    val vid = lastVersion.getVid

    // Retrieve the last version (should have value 11)
    val lastVersionWorkflow = WorkflowVersionResource.fetchWorkflowVersion(wid, vid)

    // Parse the JSON content to verify the value
    val jsonNode = objectMapper.readTree(lastVersionWorkflow.getContent)
    val op1Value = jsonNode.path("operators").path("op1").path("value").asInt()

    // The value should be 11 at the latest version
    assert(op1Value === 11)

    // Check a middle version (e.g., the 5th version)
    val midVersionId = versionMap.keys.toList.sorted.apply(4) // 5th version
    val midVersion = WorkflowVersionResource.fetchWorkflowVersion(wid, midVersionId)
    val midJsonNode = objectMapper.readTree(midVersion.getContent)
    val midValue = midJsonNode.path("operators").path("op1").path("value").asInt()

    // Value should match what we set for this version
    assert(midValue === versionMap(midVersionId))

    // Check the first version (should still be 1)
    val firstVersionWorkflow =
      WorkflowVersionResource.fetchWorkflowVersion(wid, initialVersion.getVid)
    val firstJsonNode = objectMapper.readTree(firstVersionWorkflow.getContent)
    val firstValue = firstJsonNode.path("operators").path("op1").path("value").asInt()

    // The value should be 1 at the first version
    assert(firstValue === 1)
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

    var versions: List[WorkflowVersion] = List()
    for (change <- changes) {
      val version = createVersionForWorkflow(wid, change)
      versions = versions :+ version
      Thread.sleep(50)
    }

    // Check the state at the final version (should have all changes applied)
    val finalVersion = WorkflowVersionResource.fetchWorkflowVersion(wid, versions.last.getVid)
    val finalJson = objectMapper.readTree(finalVersion.getContent)

    // Verify final state has all changes applied
    assert(finalJson.path("operators").path("op1").path("value").asInt() === 2)
    assert(!finalJson.path("operators").path("op1").has("name")) // was removed in version 6
    assert(finalJson.path("operators").has("op2"))
    assert(finalJson.path("operators").path("op2").path("value").asInt() === 10)
    assert(finalJson.has("links"))
    assert(finalJson.path("operators").path("op1").path("config").has("color"))
    assert(finalJson.path("operators").path("op1").path("config").path("color").asText() === "blue")

    // Check the initial version (should be the original content)
    val firstVersion = WorkflowVersionResource.fetchWorkflowVersion(wid, initialVersion.getVid)
    val firstJson = objectMapper.readTree(firstVersion.getContent)

    assert(firstJson.path("operators").path("op1").path("value").asInt() === 1)
    assert(!firstJson.path("operators").path("op1").has("name"))
    assert(!firstJson.path("operators").has("op2"))
    assert(!firstJson.has("links"))
    assert(firstJson.path("operators").path("op1").path("config").isObject)
    assert(firstJson.path("operators").path("op1").path("config").isEmpty)
  }

  // Test case to verify that fetchWorkflowVersion with the latest vid returns the current workflow
  "fetchWorkflowVersion" should "return the current workflow when using the latest vid" in {
    // Create a workflow with initial content
    val initialContent = """{"operators": {"op1": {"id": "op1", "value": 1}}}"""
    val updatedContent = """{"operators": {"op1": {"id": "op1", "value": 99}}}"""
    val workflow = createTestWorkflow("Test Workflow 3", initialContent)
    val wid = workflow.getWid

    // Create initial version
    val initialVersion = WorkflowVersionResource.insertNewVersion(wid)

    // Update the workflow content
    workflow.setContent(updatedContent)
    workflowDao.update(workflow)

    // Create another version after the update
    val updatedVersion = WorkflowVersionResource.insertNewVersion(wid)

    // Retrieve the latest version
    val retrievedWorkflow = WorkflowVersionResource.fetchWorkflowVersion(wid, updatedVersion.getVid)

    // Verify the content is the same as the current workflow
    assert(retrievedWorkflow.getContent === updatedContent)

    // Retrieve the initial version
    val initialWorkflow = WorkflowVersionResource.fetchWorkflowVersion(wid, initialVersion.getVid)

    // Verify the content is the original content
    assert(initialWorkflow.getContent === initialContent)
  }
}
