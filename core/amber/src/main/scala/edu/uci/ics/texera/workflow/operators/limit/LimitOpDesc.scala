package edu.uci.ics.texera.workflow.operators.limit

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.operators.limit.LimitOpDesc.equallyPartitionGoal

import scala.collection.mutable

object LimitOpDesc {

  /**
   * Tries to equally partition a integer goal into n total number of workers.
   * In the case that the goal is not a multiple of worker count,
   * this function tries to spread out the remainder evenly to the workers.
   *
   * @param goal total goal to reach for all workers
   * @param totalNumWorkers total number of workers
   * @return a list which size is equal to totalNumWorkers, each number is the goal assigned for that worker index
   */
  def equallyPartitionGoal(goal: Int, totalNumWorkers: Int): List[Int] = {
    val goalPerWorker = mutable.ArrayBuffer.fill(totalNumWorkers)(goal / totalNumWorkers) // integer division
    // divide up the remainder, give 1 to the first n workers
    for (worker <- 0 until goal % totalNumWorkers) {
      goalPerWorker(worker) = goalPerWorker(worker) + 1
    }
    goalPerWorker.toList
  }

}

class LimitOpDesc extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Limit")
  @JsonPropertyDescription("the max number of output rows")
  var limit: Int = _

  override def operatorExecutor: OpExecConfig = {
    val limitPerWorker = equallyPartitionGoal(limit, Constants.defaultNumWorkers)
    new OneToOneOpExecConfig(this.operatorIdentifier, i => new LimitOpExec(limitPerWorker(i)))
  }

  override def operatorInfo: OperatorInfo = OperatorInfo(
    "Limit", "Limit the number of output rows", OperatorGroupConstants.UTILITY_GROUP, 1, 1
  )

  override def getOutputSchema(schemas: Array[Schema]): Schema = schemas(0)
}
