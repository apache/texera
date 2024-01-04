package edu.uci.ics.amber.engine.architecture.scheduling.workerPolicies

import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{WorkflowRuntimeStatisticsDao, WorkflowVersionDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowRuntimeStatistics
import org.jooq.DSLContext

import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaBufferConverter

object HistoricalRuntimeStats {
  def apply(): HistoricalRuntimeStats = {
    HistoricalRuntimeStats()
  }
}

case class HistoricalRuntimeStats() {
  lazy val context: DSLContext = SqlServer.createDSLContext()
  val workflowRuntimeStatisticsDao = new WorkflowRuntimeStatisticsDao(context.configuration)
  val workflowVersionDao = new WorkflowVersionDao(context.configuration)

  def getHistoricalRuntimeStatsForOperator(operatorId: String): mutable.Buffer[WorkflowRuntimeStatistics] = {
    workflowRuntimeStatisticsDao.fetchByOperatorId(operatorId).asScala
  }
}