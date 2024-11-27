package edu.uci.ics.amber.engine.architecture.controller.execution

import com.rits.cloning.Cloner
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.engine.architecture.scheduling.Region
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerStatistics
import edu.uci.ics.amber.engine.common.executionruntimestate.OperatorMetrics
import edu.uci.ics.amber.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.workflow.PhysicalLink

import scala.collection.mutable

object Cloning {
  val cloner = new Cloner()
  // prevent cloner from cloning scala Nil, which it cannot handle properly
  cloner.dontClone(classOf[WorkerStatistics])
}

case class RegionExecution(region: Region) {

  private val operatorExecutions: mutable.Map[PhysicalOpIdentity, OperatorExecution] =
    mutable.HashMap()

  private val linkExecutions: mutable.Map[PhysicalLink, LinkExecution] = mutable.HashMap()

  /**
    * Initializes and retrieves an `OperatorExecution` for a given physical operatorId.
    * Optionally, an OperatorExecution instance (from other regionExecutions) can
    * be provided to make a copy.
    * If an existing `OperatorExecution` is not provided, it creates a new one.
    * An assertion error is thrown if initialization is attempted for an already existing
    * operatorId.
    *
    * @param physicalOpId             The physical operatorId for which to initialize or retrieve the execution.
    * @param inheritOperatorExecution An optional `OperatorExecution` to make a copy.
    * @return The `OperatorExecution` associated with the given physical operatorId.
    * @throws AssertionError if the `OperatorExecution` has already been initialized.
    */
  def initOperatorExecution(
      physicalOpId: PhysicalOpIdentity,
      inheritOperatorExecution: Option[OperatorExecution] = None
  ): OperatorExecution = {
    assert(!operatorExecutions.contains(physicalOpId), "OperatorExecution already exists.")

    operatorExecutions.getOrElseUpdate(
      physicalOpId,
      inheritOperatorExecution
        .map(operatorExecution => Cloning.cloner.deepClone(operatorExecution))
        .getOrElse(OperatorExecution())
    )
  }

  /**
    * Retrieves an `OperatorExecution` for the specified operatorId.
    *
    * @param opId The ID of the operator whose execution is to be retrieved.
    * @return The `OperatorExecution` associated with the specified ID.
    */
  def getOperatorExecution(opId: PhysicalOpIdentity): OperatorExecution = operatorExecutions(opId)

  /**
    * Checks if an `OperatorExecution` exists for the specified operatorId.
    *
    * @param opId The identifier of the operator to check.
    * @return True if an execution exists for the operatorId, false otherwise.
    */
  def hasOperatorExecution(opId: PhysicalOpIdentity): Boolean = operatorExecutions.contains(opId)

  /**
    * Retrieves all `OperatorExecutions` stored.
    */
  def getAllOperatorExecutions: Iterable[(PhysicalOpIdentity, OperatorExecution)] =
    operatorExecutions

  /**
    * Initializes a `LinkExecution` for a given physical link. Creates a new `LinkExecution`
    * if one does not already exist for the link.
    * An assertion error is thrown if initialization is attempted for an already existing link.
    *
    * @param link The `PhysicalLink` for which to initialize the `LinkExecution`.
    * @return The newly initialized `LinkExecution`.
    * @throws AssertionError if the `LinkExecution` has already been initialized for the link.
    */
  def initLinkExecution(link: PhysicalLink): LinkExecution = {
    assert(!linkExecutions.contains(link))
    linkExecutions.getOrElseUpdate(link, new LinkExecution())
  }

  /**
    * Retrieves all `LinkExecutions` stored.
    */
  def getAllLinkExecutions: Iterable[(PhysicalLink, LinkExecution)] = linkExecutions

  def getStats: Map[PhysicalOpIdentity, OperatorMetrics] = {
    operatorExecutions.map {
      case (physicalOpId, operatorExecution) =>
        physicalOpId -> operatorExecution.getStats
    }.toMap
  }

  def isCompleted: Boolean = getState == WorkflowAggregatedState.COMPLETED

  def getState: WorkflowAggregatedState = {
    if (
      region.getPorts.forall(globalPortId => {
        val operatorExecution = this.getOperatorExecution(globalPortId.opId)
        if (globalPortId.input) operatorExecution.isInputPortCompleted(globalPortId.portId)
        else operatorExecution.isOutputPortCompleted(globalPortId.portId)
      })
    ) {
      WorkflowAggregatedState.COMPLETED
    } else {
      WorkflowAggregatedState.RUNNING
    }
  }

}
