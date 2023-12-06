package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.linksemantics._
import edu.uci.ics.amber.engine.common.AmberConfig.defaultBatchSize
import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, LinkIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class PartitionEnforcer(physicalPlan: PhysicalPlan) {

  // a map of an operator to its output partition info
  val outputPartitionInfos = new mutable.HashMap[LayerIdentity, PartitionInfo]()
  val linkMapping = new mutable.HashMap[LinkIdentity, LinkStrategy]()

  def getOutputPartition(
      current: LayerIdentity,
      fromPort: Int,
      input: LayerIdentity,
      inputPort: Int
  ): (LinkStrategy, PartitionInfo) = {
    val layer = physicalPlan.getLayer(current)
    val inputLayer = physicalPlan.getLayer(input)

    // make sure this input is connected to this port
    assert(layer.getInputOperators(inputPort).contains(input))

    // partition requirement of this layer on this input port
    val part = layer.partitionRequirement.lift(inputPort).flatten.getOrElse(UnknownPartition())
    // output partition of the input
    val inputPart = outputPartitionInfos(input)

    // input partition satisfies the requirement, and number of worker match
    if (inputPart.satisfies(part) && inputLayer.numWorkers == layer.numWorkers) {
      val linkStrategy = new OneToOne(inputLayer, fromPort, layer, inputPort, defaultBatchSize)
      val outputPart = inputPart
      (linkStrategy, outputPart)
    } else {
      // we must re-distribute the input partitions
      val linkStrategy = part match {
        case HashPartition(hashColumnIndices) =>
          new HashBasedShuffle(
            inputLayer,
            fromPort,
            layer,
            inputPort,
            defaultBatchSize,
            hashColumnIndices
          )
        case RangePartition(rangeColumnIndices, rangeMin, rangeMax) =>
          new RangeBasedShuffle(
            inputLayer,
            fromPort,
            layer,
            inputPort,
            defaultBatchSize,
            rangeColumnIndices,
            rangeMin,
            rangeMax
          )
        case SinglePartition() =>
          new AllToOne(inputLayer, fromPort, layer, inputPort, defaultBatchSize)
        case BroadcastPartition() =>
          new AllBroadcast(inputLayer, fromPort, layer, inputPort, defaultBatchSize)
        case UnknownPartition() =>
          new FullRoundRobin(inputLayer, fromPort, layer, inputPort, defaultBatchSize)
      }
      val outputPart = part
      (linkStrategy, outputPart)
    }
  }

  def enforcePartition(): PhysicalPlan = {

    physicalPlan
      .topologicalIterator()
      .foreach(layerId => {
        val layer = physicalPlan.getLayer(layerId)
        if (physicalPlan.sourceOperators.contains(layerId)) {
          // get output partition info of the source operator
          val outPart = layer.partitionRequirement.headOption.flatten.getOrElse(UnknownPartition())
          outputPartitionInfos.put(layerId, outPart)
        } else {
          val inputPartitionsOnPort = new ArrayBuffer[PartitionInfo]()

          // for each input port, enforce partition requirement
          layer.inputPorts.indices.foreach(port => {
            // all input operators connected to this port
            val inputLayers = layer.getInputOperators(port)
            // the output partition info of each link connected from each input layer
            val outputPartitionsOfLayer = new ArrayBuffer[PartitionInfo]()

            val fromPort = physicalPlan.getUpstreamLinks(layerId).head.fromPort
            // for each input layer connected on this port
            // check partition requirement to enforce corresponding LinkStrategy
            inputLayers.foreach(inputLayer => {
              val (linkStrategy, outputPart) =
                getOutputPartition(layerId, fromPort, inputLayer, port)
              linkMapping.put(linkStrategy.id, linkStrategy)
              outputPartitionsOfLayer.append(outputPart)
            })

            assert(outputPartitionsOfLayer.size == inputLayers.size)

            val inputPartitionOnPort = outputPartitionsOfLayer.reduce((a, b) => a.merge(b))
            inputPartitionsOnPort.append(inputPartitionOnPort)
          })

          assert(inputPartitionsOnPort.size == layer.inputPorts.size)

          // derive the output partition info of this operator
          val outputPartitionInfo = layer.derivePartition(inputPartitionsOnPort.toList)
          outputPartitionInfos.put(layerId, outputPartitionInfo)
        }
      })

    // returns the complete physical plan with link strategies
    physicalPlan.copy(linkStrategies = linkMapping.toMap)
  }

}
