package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.Tables.{WORKFLOW, WORKFLOW_VERSION}
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.WorkflowExecutionsDao
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowExecutions
import org.jooq.types.UInteger

import java.sql.Timestamp
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

/**
  * This class handles inserting a new entry to the DB to store metadata information about every workflow execution
  * It also updates the entry if an execution status is updated
  */
class ExecutionsMetadataPersistService() extends LazyLogging {
  final private lazy val context = SqlServer.createDSLContext()

  private val workflowExecutionsDao = new WorkflowExecutionsDao(
    context.configuration
  )

  /**
    * This method inserts a new entry of a workflow execution in the database and returns the generated eId
    *
    * @param wid     the given workflow
    * @return generated execution ID
    */

  private def getLatestVersion(wid: UInteger): UInteger = {
    context
      .select(WORKFLOW_VERSION.VID)
      .from(WORKFLOW_VERSION)
      .leftJoin(WORKFLOW)
      .on(WORKFLOW_VERSION.WID.eq(WORKFLOW.WID))
      .where(WORKFLOW_VERSION.WID.eq(wid))
      .fetchInto(classOf[UInteger])
      .toList
      .max
  }

  def insertNewExecution(
      wid: UInteger
  ): UInteger = {
    // first retrieve the latest version of this workflow
    val vid = getLatestVersion(wid)
    val newExecution = new WorkflowExecutions()
    newExecution.setWid(wid)
    newExecution.setVid(vid)
    newExecution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    workflowExecutionsDao.insert(newExecution)
    newExecution.getEid
  }

  def updateExistingExecution(eid: UInteger, code: Byte): Unit = {
    if (eid != null) {
      val execution = workflowExecutionsDao.fetchOneByEid(eid)
      execution.setStatus(code)
      execution.setCompletionTime(new Timestamp(System.currentTimeMillis()))
      workflowExecutionsDao.update(execution)
    }
  }
}
