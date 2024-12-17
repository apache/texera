package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.workflow.WorkflowContext.DEFAULT_EXECUTION_ID
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.WorkflowExecutionsDao
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.WorkflowExecutions
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowVersionResource._
import org.jooq.types.UInteger

import java.sql.Timestamp

/**
  * This global object handles inserting a new entry to the DB to store metadata information about every workflow execution
  * It also updates the entry if an execution status is updated
  */
object ExecutionsMetadataPersistService extends LazyLogging {
  final private lazy val context = SqlServer
    .getInstance(StorageConfig.jdbcUrl, StorageConfig.jdbcUsername, StorageConfig.jdbcPassword)
    .createDSLContext()
  private val workflowExecutionsDao = new WorkflowExecutionsDao(
    context.configuration
  )

  /**
    * This method inserts a new entry of a workflow execution in the database and returns the generated eId
    *
    * @param workflowId the given workflow
    * @param uid        user id that initiated the execution
    * @return generated execution ID
    */

  def insertNewExecution(
      workflowId: WorkflowIdentity,
      uid: Option[UInteger],
      executionName: String,
      environmentVersion: String
  ): ExecutionIdentity = {
    if (!AmberConfig.isUserSystemEnabled) return DEFAULT_EXECUTION_ID
    // first retrieve the latest version of this workflow
    val vid = getLatestVersion(UInteger.valueOf(workflowId.id))
    val newExecution = new WorkflowExecutions()
    if (executionName != "") {
      newExecution.setName(executionName)
    }
    newExecution.setVid(vid)
    newExecution.setUid(uid.orNull)
    newExecution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    newExecution.setEnvironmentVersion(environmentVersion)
    workflowExecutionsDao.insert(newExecution)
    ExecutionIdentity(newExecution.getEid.longValue())
  }

  def tryGetExistingExecution(executionId: ExecutionIdentity): Option[WorkflowExecutions] = {
    if (!AmberConfig.isUserSystemEnabled) return None
    try {
      Some(workflowExecutionsDao.fetchOneByEid(UInteger.valueOf(executionId.id)))
    } catch {
      case t: Throwable =>
        logger.info("Unable to get execution. Error = " + t.getMessage)
        None
    }
  }

  def tryUpdateExistingExecution(
      executionId: ExecutionIdentity
  )(updateFunc: WorkflowExecutions => Unit): Unit = {
    if (!AmberConfig.isUserSystemEnabled) return
    try {
      val execution = workflowExecutionsDao.fetchOneByEid(UInteger.valueOf(executionId.id))
      updateFunc(execution)
      workflowExecutionsDao.update(execution)
    } catch {
      case t: Throwable =>
        logger.info("Unable to update execution. Error = " + t.getMessage)
    }
  }
}
