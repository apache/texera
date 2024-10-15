package edu.uci.ics.amber.engine.architecture.deploysemantics.locationpreference

import akka.actor.Address

case class AddressInfo(
    // the addresses of all worker nodes
    allAddresses: Array[Address],
    // the address of the controller
    controllerAddress: Address
)

sealed trait LocationPreference extends Serializable

object PreferController extends LocationPreference

object RoundRobinPreference extends LocationPreference
