package edu.uci.ics.amber.engine.architecture.deploysemantics.locationpreference

import akka.actor.Address
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig

case class AddressInfo(
    // the addresses of all worker nodes
    allAddresses: Array[Address],
    // the address of the controller
    controllerAddress: Address
)

trait LocationPreference extends Serializable {

  def getPreferredLocation(
      addressInfo: AddressInfo,
      workerLayer: OpExecConfig,
      workerIndex: Int
  ): Address

}

class PreferController extends LocationPreference {
  override def getPreferredLocation(
      addressInfo: AddressInfo,
      workerLayer: OpExecConfig,
      workerIndex: Int
  ): Address = {
    addressInfo.controllerAddress
  }
}

class RoundRobinPreference extends LocationPreference {
  override def getPreferredLocation(
      addressInfo: AddressInfo,
      workerLayer: OpExecConfig,
      workerIndex: Int
  ): Address = {
    addressInfo.allAddresses(workerIndex % addressInfo.allAddresses.length)
  }
}
