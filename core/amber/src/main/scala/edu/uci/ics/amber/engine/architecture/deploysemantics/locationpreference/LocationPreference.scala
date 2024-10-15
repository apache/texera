package edu.uci.ics.amber.engine.architecture.deploysemantics.locationpreference

import akka.actor.Address

// Holds worker and controller node addresses.
case class AddressInfo(
    allAddresses: Array[Address], // e.g., Node 1, Node 2, Node 3
    controllerAddress: Address // Controller node
)

// LocationPreference defines where operators should run.
sealed trait LocationPreference extends Serializable

// PreferController: Run on the controller node.
// Example: For scan operators reading files or sink operators writing results on the controller.
object PreferController extends LocationPreference

// RoundRobinPreference: Distribute across worker nodes, per operator.
// Example:
// - Operator A: Worker 1 -> Node 1, Worker 2 -> Node 2, Worker 3 -> Node 3
// - Operator B: Worker 1 -> Node 1, Worker 2 -> Node 2
object RoundRobinPreference extends LocationPreference
