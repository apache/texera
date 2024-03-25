package edu.uci.ics.amber.engine.architecture.deploysemantics.locationpreference

import akka.actor.Address
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp

case class AddressInfo(
    // the addresses of all worker nodes
    allAddresses: Array[Address],
    // the address of the controller
    controllerAddress: Address
)

trait LocationPreference extends Serializable {

  def getPreferredLocation(
      addressInfo: AddressInfo,
      physicalOp: PhysicalOp,
      workerIndex: Int
  ): Address

}

class PreferController extends LocationPreference {
  override def getPreferredLocation(
      addressInfo: AddressInfo,
      physicalOp: PhysicalOp,
      workerIndex: Int
  ): Address = {
    addressInfo.controllerAddress
  }
}

class RoundRobinPreference extends LocationPreference {
  override def getPreferredLocation(
      addressInfo: AddressInfo,
      physicalOp: PhysicalOp,
      workerIndex: Int
  ): Address = {
    assert(
      addressInfo.allAddresses.nonEmpty,
      "Execution failed to start, no available computation nodes"
    )
    addressInfo.allAddresses(workerIndex % addressInfo.allAddresses.length)
  }
}
