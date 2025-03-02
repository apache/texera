package edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies

import edu.uci.ics.amber.core.storage.DocumentFactory
import edu.uci.ics.amber.core.storage.VFSURIFactory.{createResultURI, decodeURI}
import edu.uci.ics.amber.core.storage.result.ExecutionResourcesMapping
import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PartitionInfo, PhysicalLink, PhysicalPlan, PortIdentity, UnknownPartition, WorkflowContext}
import edu.uci.ics.amber.engine.architecture.scheduling.Region
import edu.uci.ics.amber.engine.architecture.scheduling.config.ChannelConfig.generateChannelConfigs
import edu.uci.ics.amber.engine.architecture.scheduling.config.LinkConfig.toPartitioning
import edu.uci.ics.amber.engine.architecture.scheduling.config.WorkerConfig.generateWorkerConfigs
import edu.uci.ics.amber.engine.architecture.scheduling.config.{LinkConfig, OperatorConfig, ResourceConfig}
import edu.uci.ics.amber.core.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource

import java.net.URI
import scala.collection.mutable

trait ResourceAllocator {
  def allocate(region: Region): (Region, Double)
}

class DefaultResourceAllocator(
    physicalPlan: PhysicalPlan,
    executionClusterInfo: ExecutionClusterInfo,
    workflowContext: WorkflowContext
) extends ResourceAllocator {

  // a map of a physical link to the partition info of the upstream/downstream of this link
  private val linkPartitionInfos = new mutable.HashMap[PhysicalLink, PartitionInfo]()

  private val operatorConfigs = new mutable.HashMap[PhysicalOpIdentity, OperatorConfig]()
  private val linkConfigs = new mutable.HashMap[PhysicalLink, LinkConfig]()

  /**
    * Allocates resources for a given region and its operators.
    *
    * This method calculates and assigns worker configurations for each operator
    * in the region. For the operators that are parallelizable, it respects the
    * suggested worker number if provided. Otherwise, it falls back to a default
    * value. Non-parallelizable operators are assigned a single worker.
    *
    * @param region The region for which to allocate resources.
    * @return A tuple containing:
    *         1) A new Region instance with new resource configuration.
    *         2) An estimated cost of the workflow with the new resource configuration,
    *         represented as a Double value (currently set to 0, but will be
    *         updated in the future).
    */
  def allocate(
      region: Region
  ): (Region, Double) = {

    val resultURIMap = createOutputPortStorageObjects(region)

    val opToOperatorConfigMapping = region.getOperators
      .map(physicalOp => physicalOp.id -> OperatorConfig(generateWorkerConfigs(physicalOp)))
      .toMap

    operatorConfigs ++= opToOperatorConfigMapping

    propagatePartitionRequirement(region)

    val linkToLinkConfigMapping = region.getLinks.map { physicalLink =>
      physicalLink -> LinkConfig(
        generateChannelConfigs(
          operatorConfigs(physicalLink.fromOpId).workerConfigs.map(_.workerId),
          operatorConfigs(physicalLink.toOpId).workerConfigs.map(_.workerId),
          toPortId = physicalLink.toPortId,
          linkPartitionInfos(physicalLink)
        ),
        toPartitioning(
          operatorConfigs(physicalLink.fromOpId).workerConfigs.map(_.workerId),
          operatorConfigs(physicalLink.toOpId).workerConfigs.map(_.workerId),
          linkPartitionInfos(physicalLink),
          workflowContext.workflowSettings.dataTransferBatchSize
        )
      )
    }.toMap

    linkConfigs ++= linkToLinkConfigMapping

    val resourceConfig = ResourceConfig(opToOperatorConfigMapping, linkToLinkConfigMapping)

    (region.copy(resourceConfig = Some(resourceConfig), outputPortResultURIs = resultURIMap), 0)
  }

  private def createOutputPortStorageObjects(region: Region): Map[GlobalPortIdentity, URI] = {
    val resultURIMap: mutable.HashMap[GlobalPortIdentity, URI] = mutable.HashMap.empty

    // Create storage objects
    region.materializedPortIds.foreach(outputPortId => {
      val storageUriToAdd = createResultURI(
        workflowId = workflowContext.workflowId,
        executionId = workflowContext.executionId,
        operatorId = outputPortId.opId.logicalOpId,
        layerName = Some(outputPortId.opId.layerName),
        portIdentity = outputPortId.portId
      )
      val existingStorageUri =
        WorkflowExecutionsResource.getResultUriByExecutionAndPort(
          wid = workflowContext.workflowId,
          eid = workflowContext.executionId,
          opId = outputPortId.opId.logicalOpId,
          layerName = Some(outputPortId.opId.layerName),
          portId = outputPortId.portId
        )
      if (
        (!AmberConfig.isUserSystemEnabled && !ExecutionResourcesMapping
          .getResourceURIs(workflowContext.executionId)
          .contains(
            existingStorageUri
          )) || (AmberConfig.isUserSystemEnabled && existingStorageUri.isEmpty)
      ) {
        // Avoid duplicate creation bacause of operators with dependee inputs belonging to two regions
        val schemaOptional = region.getOperator(outputPortId.opId).outputPorts(outputPortId.portId)._3
        val schema = schemaOptional.getOrElse(throw new IllegalStateException("Schema is missing"))
        DocumentFactory.createDocument(storageUriToAdd, schema)
        WorkflowExecutionsResource.insertOperatorPortResultUri(
          eid = workflowContext.executionId,
          opId = outputPortId.opId.logicalOpId,
          layerName = outputPortId.opId.layerName,
          portId = outputPortId.portId,
          storageUriToAdd
        )

        // TODO: Add storage URI as part of region
        resultURIMap(outputPortId) = storageUriToAdd
      }
    })
    resultURIMap.toMap

//    region.getOperators.foreach(physicalOp =>
//      physicalOp.outputPorts.foreach {
//        case (outputPortId, (outputPort, _, schemaOptional)) =>
//          if (outputPort.storageUri != "") {
//            // Assign storage object.
//            val storageUriToAdd = new URI(outputPort.storageUri)
//            val (workflowId, executionId, logicalOpId, layerName, _, _) = decodeURI(storageUriToAdd)
//            val existingStorageUri =
//              WorkflowExecutionsResource.getResultUriByExecutionAndPort(
//                workflowId,
//                executionId,
//                logicalOpId.get,
//                layerName,
//                outputPortId
//              )
//            if (
//              (!AmberConfig.isUserSystemEnabled && !ExecutionResourcesMapping
//                .getResourceURIs(executionId)
//                .contains(
//                  existingStorageUri
//                )) || (AmberConfig.isUserSystemEnabled && existingStorageUri.isEmpty)
//            ) {
//              // Avoid duplicate creation bacause of operators with dependee inputs belonging to two regions
//              val schema =
//                schemaOptional.getOrElse(throw new IllegalStateException("Schema is missing"))
//              DocumentFactory.createDocument(storageUriToAdd, schema)
//              WorkflowExecutionsResource.insertOperatorPortResultUri(
//                executionId,
//                logicalOpId.get,
//                layerName.get,
//                outputPortId,
//                storageUriToAdd
//              )
//            }
//          }
//      }
//    )
  }

  /**
    * This method propagates partitioning requirements in the PhysicalPlan DAG.
    *
    * This method is invoked once for each region, and only propagate partitioning requirements within
    * the region. For example, suppose we have the following physical Plan:
    *
    * A ->
    * HJ
    * B ->
    * The link A->HJ will be propagated in the first region. The link B->HJ will be propagated in the second region.
    * The output partition info of HJ will be derived after both links are propagated, which is in the second region.
    */
  private def propagatePartitionRequirement(region: Region): Unit = {
    region
      .topologicalIterator()
      .foreach(physicalOpId => {
        val physicalOp = region.getOperator(physicalOpId)
        val outputPartitionInfo = if (physicalPlan.getSourceOperatorIds.contains(physicalOpId)) {
          Some(physicalOp.partitionRequirement.headOption.flatten.getOrElse(UnknownPartition()))
        } else {
          val inputPartitionInfos = physicalOp.inputPorts.keys
            .flatMap((portId: PortIdentity) => {
              physicalOp
                .getInputLinks(Some(portId))
                .filter(link => region.getLinks.contains(link))
                .map(link => {
                  val previousLinkPartitionInfo =
                    linkPartitionInfos.getOrElse(link, UnknownPartition())
                  val updatedLinkPartitionInfo = physicalPlan.getOutputPartitionInfo(
                    link,
                    previousLinkPartitionInfo,
                    operatorConfigs.map {
                      case (opId, operatorConfig) => opId -> operatorConfig.workerConfigs.length
                    }.toMap
                  )
                  linkPartitionInfos.put(link, updatedLinkPartitionInfo)
                  (link.toPortId, updatedLinkPartitionInfo)
                })
            })
            // group upstream partition infos by input port of this physicalOp
            .groupBy(_._1)
            .values
            .toList
            // if there are multiple partition infos on an input port, reduce them to once
            .map(_.map(_._2).reduce((p1, p2) => p1.merge(p2)))

          if (inputPartitionInfos.length == physicalOp.inputPorts.size) {
            // derive the output partition info with all the input partition infos
            Some(physicalOp.derivePartition(inputPartitionInfos))
          } else {
            None
          }

        }

        if (outputPartitionInfo.isDefined) {
          physicalOp.outputPorts.keys
            .flatMap(physicalOp.getOutputLinks)
            .foreach(link =>
              // by default, a link's partition info comes from its input, unless updated to match its output.
              linkPartitionInfos.put(link, outputPartitionInfo.get)
            )
        }
      })
  }
}
