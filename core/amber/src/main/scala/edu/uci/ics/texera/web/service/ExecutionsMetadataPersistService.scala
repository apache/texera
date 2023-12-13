package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.Utils.maptoStatusCode
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.WorkflowExecutionsDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowExecutions
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowVersionResource._
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState
import edu.uci.ics.texera.workflow.common.WorkflowContext.DEFAULT_EXECUTION_ID
import org.jooq.types.UInteger

import java.sql.Timestamp

/**
  * This global object handles inserting a new entry to the DB to store metadata information about every workflow execution
  * It also updates the entry if an execution status is updated
  */
object ExecutionsMetadataPersistService extends LazyLogging {
  final private lazy val context = SqlServer.createDSLContext()
  private final val userSystemEnabled: Boolean = AmberConfig.isUserSystemEnabled
  private val workflowExecutionsDao = new WorkflowExecutionsDao(
    context.configuration
  )

  /**
    * This method inserts a new entry of a workflow execution in the database and returns the generated eId
    *
    * @param wid     the given workflow
    * @param uid     user id that initiated the execution
    * @return generated execution ID
    */

  def insertNewExecution(
      wid: UInteger,
      uid: Option[UInteger],
      executionName: String,
      environmentVersion: String
  ): Long = {
    if (!userSystemEnabled) return DEFAULT_EXECUTION_ID
    // first retrieve the latest version of this workflow
    val vid = getLatestVersion(wid)
    val newExecution = new WorkflowExecutions()
    if (executionName != "") {
      newExecution.setName(executionName)
    }
    newExecution.setVid(vid)
    newExecution.setUid(uid.orNull)
    newExecution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    newExecution.setEnvironmentVersion(environmentVersion)
    workflowExecutionsDao.insert(newExecution)
    newExecution.getEid.longValue()
  }

  def tryUpdateExistingExecution(eid: Long, state: WorkflowAggregatedState): Unit = {
    if (!userSystemEnabled) return
    try {
      val code = maptoStatusCode(state)
      val execution = workflowExecutionsDao.fetchOneByEid(UInteger.valueOf(eid))
      execution.setStatus(code)
      execution.setLastUpdateTime(new Timestamp(System.currentTimeMillis()))
      workflowExecutionsDao.update(execution)
    } catch {
      case t: Throwable =>
        logger.info("Unable to update execution. Error = " + t.getMessage)
    }
  }

  def tryUpdateExecutionStatusAndPointers(eid: Long, state: WorkflowAggregatedState): Unit = {
    if (!userSystemEnabled) return
    try {
      val code = maptoStatusCode(state)
      val execution = workflowExecutionsDao.fetchOneByEid(UInteger.valueOf(eid))
      execution.setStatus(code)
      execution.setResult("")
      workflowExecutionsDao.update(execution)
    } catch {
      case t: Throwable =>
        logger.info("Unable to update execution. Error = " + t.getMessage)
    }
  }

  def updateExistingExecutionVolumePointers(eid: Long, pointers: String): Unit = {
    if (!userSystemEnabled) return
    try {
      val execution = workflowExecutionsDao.fetchOneByEid(UInteger.valueOf(eid))
      execution.setResult(pointers)
      workflowExecutionsDao.update(execution)
    } catch {
      case t: Throwable =>
        logger.info("Unable to update execution. Error = " + t.getMessage)
    }
  }
}
