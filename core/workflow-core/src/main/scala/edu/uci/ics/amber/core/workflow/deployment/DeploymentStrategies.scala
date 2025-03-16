package edu.uci.ics.amber.core.workflow.deployment

import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.core.workflow.PhysicalOp
import edu.uci.ics.amber.core.workflow.deployment.NodeProfiles.Profile

import scala.collection.{immutable, mutable}
import scala.collection.mutable.Map
import scala.util.Random

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
    if (currentOp.isSourceOperator) {
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
    else {
      val inputOps = currentOp.getInputLinks().map(_.fromOpId)
      if (inputOps != List.empty) {
        //Get addresses of input ops
        val assignedWorkers: List[String] = inputOps.flatMap(opId => operatorMapping.get(opId))
        val profiles: List[Profile] = assignedWorkers.flatMap(workerAddr => nodeProfiles.get(workerAddr))
        if (profiles != List.empty) {
          val bestNode = profiles.maxBy(profile => profile.modelSize)
          operatorMapping + (currentOp.id -> bestNode.nodeAddress)
          return bestNode.nodeAddress
        }
      }
      val outputOps = currentOp.getInputLinks().map(_.fromOpId)
      if (outputOps != List.empty) {
        //Get addresses of output ops
        val assignedWorkers: List[String] = outputOps.flatMap(opId => operatorMapping.get(opId))
        val profiles: List[Profile] = assignedWorkers.flatMap(workerAddr => nodeProfiles.get(workerAddr))
        if (profiles != List.empty) {
          val bestNode = profiles.maxBy(profile => profile.modelSize)
          operatorMapping + (currentOp.id -> bestNode.nodeAddress)
          return bestNode.nodeAddress
        }
      }
    }
    //Default to free worker to leverage parallelization
    //If no free workers, default to fastest
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
  // balance between quality and speed
  def hybrid(
              addresses: Array[String],
              nodeProfiles: immutable.Map[String, Profile],
              operators: Set[PhysicalOp],
              currentOp: PhysicalOp,
              operatorMapping: mutable.Map[PhysicalOpIdentity, String]
            ): String = {
    //Leverage parallelization, but go for better models
    if(currentOp.isSourceOperator){
      //Get models ordered by speed
      val sortedAddresses = nodeProfiles.toSeq
        .sortBy { case (_, profile) => profile.modelSize }(Ordering[Int].reverse)
        .map { case (address, _) => address }

      //Assign k source vertices to k fastest nodes
      sortedAddresses.find(addr => !operatorMapping.values.toSet.contains(addr)) match {
        case Some(freeAddr) =>
          operatorMapping += (currentOp.id -> freeAddr)
          freeAddr
        //Default to biggest model if there are more source vertices than workers
        case None =>
          val fallback = sortedAddresses.head
          operatorMapping + (currentOp.id -> fallback)
          fallback
      }
    }
    else {
      val inputOps = currentOp.getInputLinks().map(_.fromOpId)
      if (inputOps != List.empty) {
        //Get addresses of input ops
        val assignedWorkers: List[String] = inputOps.flatMap(opId => operatorMapping.get(opId))
        val profiles: List[Profile] = assignedWorkers.flatMap(workerAddr => nodeProfiles.get(workerAddr))
        if (profiles != List.empty) {
          val bestNode = profiles.maxBy(profile => profile.modelSize)
          operatorMapping + (currentOp.id -> bestNode.nodeAddress)
          return bestNode.nodeAddress
        }
      }
      val outputOps = currentOp.getInputLinks().map(_.fromOpId)
      if (outputOps != List.empty) {
        //Get addresses of output ops
        val assignedWorkers: List[String] = outputOps.flatMap(opId => operatorMapping.get(opId))
        val profiles: List[Profile] = assignedWorkers.flatMap(workerAddr => nodeProfiles.get(workerAddr))
        if (profiles != List.empty) {
          val bestNode = profiles.maxBy(profile => profile.modelSize)
          operatorMapping + (currentOp.id -> bestNode.nodeAddress)
          return bestNode.nodeAddress
        }
      }
    }
    val returnedAddress = addresses(Random.nextInt(addresses.length))
    operatorMapping += (currentOp.id -> returnedAddress )
    returnedAddress
  }
}
