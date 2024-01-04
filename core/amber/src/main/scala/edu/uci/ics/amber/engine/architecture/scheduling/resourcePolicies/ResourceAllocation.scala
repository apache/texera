package edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies

import edu.uci.ics.amber.engine.architecture.scheduling.Region
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

import scala.collection.mutable.ListBuffer

object ResourceAllocation {
  def apply(region: Region): ResourceAllocation = {
    ResourceAllocation(region)
  }
}

case class ResourceAllocation(region: Region) {

  private def physicalOpIds = region.physicalOpIds.filter(_.logicalOpId.id.contains("PythonUDF"))

  def allocation(
      physicalPlan: PhysicalPlan,
      executionClusterInfo: ExecutionClusterInfo
  ): (List[Int], Double) = {
    val workers = ListBuffer[Int]()
    physicalOpIds.foreach { opId =>
      val operator = physicalPlan.getOperator(opId)

      if (operator.parallelizable) {
        operator.suggestedWorkerNum match {
          case Some(num) => workers += num
          case None =>
            workers += AmberConfig.numWorkerPerOperatorByDefault // Set to default value if not defined
        }
      } else {
        // Set to 1 if not parallelizable
        workers += 1
      }
    }
    (workers.toList, 0)
  }
}
