package edu.uci.ics.texera.web.resource.dashboard.file

import edu.uci.ics.texera.web.MockTexeraDB
import edu.uci.ics.texera.web.auth.SessionUser
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.{User, Workflow}
import org.jooq.types.UInteger
import edu.uci.ics.texera.web.model.jooq.generated.enums.UserRole
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{UserDao, WorkflowDao}
import edu.uci.ics.texera.web.resource.dashboard.workflow.WorkflowResource
import edu.uci.ics.texera.Utils
import java.nio.file.{Files, Path, Paths}
import java.nio.charset.StandardCharsets
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import java.util

class WorkflowResourceSpec extends AnyFlatSpec with BeforeAndAfterAll with MockTexeraDB {

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

  private val testWorkflow1: Workflow = {
    val workflow = new Workflow()
    workflow.setName("test_workflow1")
    workflow.setDescription("keyword_in_workflow_description")
    workflow.setContent("{\"x\":5,\"y\":\"keyword_in_workflow_content\",\"z\":\"text phrases\"}")

    workflow
  }

  private val testWorkflow1_duplicate: Workflow = {
    val workflow = new Workflow()
    workflow.setName("test_workflow1")
    workflow.setDescription("keyword_in_workflow_description")
    workflow.setContent("{\"x\":5,\"y\":\"keyword_in_workflow_content\",\"z\":\"text phrases\"}")

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

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  "/search API " should "be able to search for workflow in different columns from Workflow table" in {
    // populate sample workflow data
    val workflow: Workflow = workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    assert(workflow.getName().equals("test_workflow1"))
    // search
    val keywords = new util.ArrayList[String]()
    keywords.add("keyword_in_workflow_content")
    var DashboardWorkflowEntryList = workflowResource.searchWorkflows(sessionUser1, keywords)
    assert(DashboardWorkflowEntryList(0).ownerName.equals("test_user"))
    assert(DashboardWorkflowEntryList(0).workflow.getName.equals("test_workflow1"))

    keywords.clear()
    keywords.add("keyword_in_workflow_description")
    DashboardWorkflowEntryList = workflowResource.searchWorkflows(sessionUser1, keywords)
    assert(DashboardWorkflowEntryList(0).ownerName.equals("test_user"))
    assert(DashboardWorkflowEntryList(0).workflow.getName.equals("test_workflow1"))
  }

  it should "be able to search text phrases" in {
    // populate sample workflow data
    val workflow: Workflow = workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    assert(workflow.getName().equals("test_workflow1"))

    val keywords = new util.ArrayList[String]()
    keywords.add("keyword_in_workflow_content")
    val DashboardWorkflowEntryList = workflowResource.searchWorkflows(sessionUser1, keywords)
    assert(DashboardWorkflowEntryList(0).workflow.getName.equals("test_workflow1"))
  }

  it should "return an empty list when given an empty list of keywords" in {
    // populate sample workflow data
    val workflow: Workflow = workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    assert(workflow.getName().equals("test_workflow1"))

    // search with empty keywords
    val keywords = new util.ArrayList[String]()
    val DashboardWorkflowEntryList = workflowResource.searchWorkflows(sessionUser1, keywords)
    println(DashboardWorkflowEntryList.length)
    assert(DashboardWorkflowEntryList.isEmpty)
  }

  it should "be able to search with multiple keywords in different combinations" in {
    // populate sample workflow data
    val workflow: Workflow = workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    assert(workflow.getName().equals("test_workflow1"))

    // search with multiple keywords
    val keywords = new util.ArrayList[String]()
    keywords.add("keyword_in_workflow_content")
    keywords.add("keyword_in_workflow_description")
    val DashboardWorkflowEntryList = workflowResource.searchWorkflows(sessionUser1, keywords)
    assert(DashboardWorkflowEntryList.size == 1)
    assert(DashboardWorkflowEntryList(0).ownerName.equals("test_user"))
    assert(DashboardWorkflowEntryList(0).workflow.getName.equals("test_workflow1"))
  }

  it should "handle reserved characters in the keywords" in {
    // populate sample workflow data
    val workflow: Workflow = workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    assert(workflow.getName().equals("test_workflow1"))

    // search with reserved characters in keywords
    val keywords = new util.ArrayList[String]()
    keywords.add("keyword_in_workflow_content+-@()<>~*\"")
    val DashboardWorkflowEntryList = workflowResource.searchWorkflows(sessionUser1, keywords)
    assert(DashboardWorkflowEntryList.size == 1)
    assert(DashboardWorkflowEntryList(0).ownerName.equals("test_user"))
    assert(DashboardWorkflowEntryList(0).workflow.getName.equals("test_workflow1"))
  }

  it should "not be able to search workflows from different user accounts" in {
    // populate sample workflow data
    workflowResource.persistWorkflow(testWorkflow1, sessionUser1)
    workflowResource.persistWorkflow(testWorkflow1_duplicate, sessionUser2)

    // search with reserved characters in keywords
    val keywords = new util.ArrayList[String]()
    keywords.add("keyword_in_workflow_content\"")
    val DashboardWorkflowEntryList = workflowResource.searchWorkflows(sessionUser2, keywords)
    assert(DashboardWorkflowEntryList.size == 1)
    assert(DashboardWorkflowEntryList(0).ownerName.equals("test_user2"))
    assert(DashboardWorkflowEntryList(0).workflow.getName.equals("test_workflow1"))
  }

}
