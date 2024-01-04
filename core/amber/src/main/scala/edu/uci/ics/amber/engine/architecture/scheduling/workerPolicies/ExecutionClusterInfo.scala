package edu.uci.ics.amber.engine.architecture.scheduling.workerPolicies

import edu.uci.ics.amber.engine.architecture.scheduling.Region
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.{WorkflowRuntimeStatisticsDao, WorkflowVersionDao}
import edu.uci.ics.texera.web.model.jooq.generated.tables.pojos.WorkflowRuntimeStatistics
import org.jooq.DSLContext

import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaBufferConverter

object ExecutionClusterInfo {
  def apply(): ExecutionClusterInfo = {
    ExecutionClusterInfo()
  }
}

case class ExecutionClusterInfo() {
  private val numCores = 4
  private val workersPerCore = 4

  def getRegionExecutionClusterInfo(region: Region): Int = {
    numCores * workersPerCore
  }
}