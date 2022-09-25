package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.sendsemantics.partitionings.Partitioning
import edu.uci.ics.amber.engine.common.virtualidentity.LayerIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.collection.mutable

//case class Hash

class PartitionEnforcer(physicalPlan: PhysicalPlan) {

  def enforcePartition(): PhysicalPlan = {
    // a map from an operator to the list of its input schema
    val outputPartitionRequirement = new mutable.HashMap[LayerIdentity, Partitioning.NonEmpty]()

    val inputSchemaMap =
      new mutable.HashMap[OperatorDescriptor, mutable.MutableList[Option[Schema]]]()
        .withDefault(op => mutable.MutableList.fill(op.operatorInfo.inputPorts.size)(Option.empty))

    //    physicalPlan.topologicalIterator()

    null
  }

}
