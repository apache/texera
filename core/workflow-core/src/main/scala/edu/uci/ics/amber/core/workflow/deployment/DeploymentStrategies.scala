package edu.uci.ics.amber.core.workflow.deployment

import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.core.workflow.PhysicalOp
import edu.uci.ics.amber.core.workflow.deployment.NodeProfiles.Profile

import scala.collection.{immutable, mutable}
import scala.collection.mutable.Map

// for nodeProfiles, please see core/workflow-core/src/main/resources/profile.conf
//  Currently the profile needs to be filled in manually. You may also fill in dummny values for testing purpose
object DeploymentStrategies {
  // maximize the quality of each answer
  def maxQuality(
      addresses: Array[String],
      nodeProfiles: immutable.Map[String, Profile],
      operators: Set[PhysicalOp],
      currentOp: PhysicalOp,
  ): String = {
    //Idea: Assign all vertices to best model
    val bestNode = nodeProfiles.maxBy { case (_, profile) => profile.modelSize }
    bestNode._1
  }

  // maximize the speed of the answer generation
  def maxSpeed(
      addresses: Array[String],
      nodeProfiles: immutable.Map[String, Profile],
      operators: Set[PhysicalOp],
      currentOp: PhysicalOp,
      operatorMapping: mutable.Map[PhysicalOpIdentity, String]
  ): String = {

    //Parallelize Source operators
    if(currentOp.isSourceOperator){
      //Get models ordered by speed
      val sortedAddresses = nodeProfiles.toSeq
        .sortBy { case (_, profile) => profile.modelSpeed }(Ordering[Float].reverse)
        .map { case (address, _) => address }

      //Assign k source vertices to k fastest nodes
      sortedAddresses.find(addr => !operatorMapping.values.toSet.contains(addr)) match {
        case Some(freeAddr) =>
          operatorMapping += (currentOp.id -> freeAddr)
          freeAddr
        //Default to fastest worker if there are more source vertices than workers
        case None =>
          val fallback = sortedAddresses.head
          operatorMapping + (currentOp.id -> fallback)
          fallback
      }
    }
    else{
      val inputOps = currentOp.getInputLinks().map(_.fromOpId)
      if(inputOps != List.empty){
        //Get addresses of input ops
        val assignedWorkers: List[String] = inputOps.flatMap(opId => operatorMapping.get(opId))
        val profiles: List[Profile] = assignedWorkers.flatMap(workerAddr => nodeProfiles.get(workerAddr))
        if(profiles != List.empty) {
          val bestNode = profiles.maxBy(profile => profile.modelSize)
          bestNode.nodeAddress
        }
      }
      val outputOps = currentOp.getInputLinks().map(_.fromOpId)
      if(outputOps != List.empty){
        //TODO implement strategy for outdeg > 1
        addresses.head
      }
      //Default to fastest node
      val defaultNode = nodeProfiles.maxBy { case (_, profile) => profile.modelSpeed }
      defaultNode._1
    }
  }

  // balance between quality and speed
  def hybrid(
      addresses: Array[String],
      nodeProfiles: immutable.Map[String, Profile],
      operators: Set[PhysicalOp],
      currentOp: PhysicalOp,
      operatorMapping: mutable.Map[PhysicalOpIdentity, String]
  ): String = {
    // dummy implementation: always return the first address
    //Select model with max size
    val bestNode = nodeProfiles.maxBy { case (_, profile) => profile.modelSize }
    // Return the address of that node
    bestNode._1
  }
}
