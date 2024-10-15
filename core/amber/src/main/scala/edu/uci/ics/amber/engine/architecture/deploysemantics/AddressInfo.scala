package edu.uci.ics.amber.engine.architecture.deploysemantics

import akka.actor.Address

// Holds worker and controller node addresses.
case class AddressInfo(
    allAddresses: Array[Address], // e.g., Node 1, Node 2, Node 3
    controllerAddress: Address // Controller node
)
