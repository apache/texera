package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.ExecutionState
import edu.uci.ics.amber.engine.architecture.scheduling.{GlobalPortIdentity, Region, RegionIdentity, RegionPlan}

import scala.collection.mutable

class RegionExecutionState {

  val completedRegions = new mutable.HashSet[Region]()
  // regions currently running
  val runningRegions = new mutable.HashSet[Region]()

   val completedPortIdsOfRegion
      : mutable.HashMap[RegionIdentity, mutable.HashSet[GlobalPortIdentity]] = mutable.HashMap()
   def isRegionCompleted(
      executionState: ExecutionState,
      region: Region
  ): Boolean = {
     region.getPorts.forall(
       globalPortId => {
         val operatorExecution = executionState.getOperatorExecution(globalPortId.opId)
         if (globalPortId.input) operatorExecution.isInputPortCompleted(globalPortId.portId)
         else operatorExecution.isOutputPortCompleted(globalPortId.portId)
       }
     )
  }

  def getRegion(portId: GlobalPortIdentity): Option[Region] = {
    runningRegions.find(r => r.getPorts.contains(portId))
  }


  def addToRunningRegions(regions: Set[Region]): Unit = {
    runningRegions ++= regions
  }

  def getCompletedRegions: Set[Region] = completedRegions.toSet

}
