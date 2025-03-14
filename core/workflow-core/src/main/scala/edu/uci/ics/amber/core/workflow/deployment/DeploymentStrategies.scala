package edu.uci.ics.amber.core.workflow.deployment

import edu.uci.ics.amber.core.workflow.PhysicalOp
import edu.uci.ics.amber.core.workflow.deployment.NodeProfiles.Profile

// for nodeProfiles, please see core/workflow-core/src/main/resources/profile.conf
//  Currently the profile needs to be filled in manually. You may also fill in dummny values for testing purpose
object DeploymentStrategies {
  // maximize the quality of each answer
  def maxQuality(
      addresses: Array[String],
      nodeProfiles: Map[String, Profile],
      operators: Set[PhysicalOp],
      currentOp: PhysicalOp
  ): String = {
    //Select model with max size
    val bestNode = nodeProfiles.maxBy { case (_, profile) => profile.modelSize }
    // Return the address of that node
    bestNode._1
  }

  // maximize the speed of the answer generation
  def maxSpeed(
      addresses: Array[String],
      nodeProfiles: Map[String, Profile],
      operators: Set[PhysicalOp],
      currentOp: PhysicalOp
  ): String = {
    // dummy implementation: always return the first address
    addresses.head
    // TODO: add real implementation
  }

  // balance between quality and speed
  def hybrid(
      addresses: Array[String],
      nodeProfiles: Map[String, Profile],
      operators: Set[PhysicalOp],
      currentOp: PhysicalOp
  ): String = {
    // dummy implementation: always return the first address
    addresses.head
    // TODO: add real implementation
  }
}
