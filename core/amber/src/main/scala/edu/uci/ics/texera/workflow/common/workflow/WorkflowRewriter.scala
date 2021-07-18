package edu.uci.ics.texera.workflow.common.workflow

import com.typesafe.scalalogging.Logger
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowRewriter.copyOperator
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import org.apache.commons.lang3.builder.EqualsBuilder

import scala.collection.mutable

class WorkflowVertex(
    val op: OperatorDescriptor,
    val links: mutable.HashSet[OperatorLink],
    val breakpoints: mutable.HashSet[BreakpointInfo]
) {
  override def equals(obj: Any): Boolean = EqualsBuilder.reflectionEquals(this, obj)

  override def toString: String = objectMapper.writeValueAsString(this)
}

object WorkflowRewriter {
  private def copyOperator(operator: OperatorDescriptor): OperatorDescriptor = {
    objectMapper.readValue(objectMapper.writeValueAsString(operator), classOf[OperatorDescriptor])
  }
}

class WorkflowRewriter(
    var workflowInfo: WorkflowInfo,
    var operatorOutputCache: mutable.HashMap[String, mutable.MutableList[Tuple]],
    var cachedOperatorDescriptors: mutable.HashMap[String, OperatorDescriptor],
    var cacheSourceOperatorDescriptors: mutable.HashMap[String, CacheSourceOpDesc],
    var cacheSinkOperatorDescriptors: mutable.HashMap[String, CacheSinkOpDesc]
) {
  private val logger = Logger(this.getClass.getName)

  var operatorRecord: mutable.HashMap[String, WorkflowVertex] = _
//  var operatorRecord: mutable.HashMap[String, OperatorDescriptor] = _

  private val workflowDAG: WorkflowDAG = if (workflowInfo != null) {
    new WorkflowDAG(workflowInfo)
  } else {
    null
  }
  private var newOperatorDescriptors = if (workflowInfo != null) {
    mutable.MutableList[OperatorDescriptor]()
  } else {
    null
  }
  private val newOperatorLinks = if (workflowInfo != null) {
    mutable.MutableList[OperatorLink]()
  } else {
    null
  }
  private val newBreakpointInfos = if (workflowInfo != null) {
    mutable.MutableList[BreakpointInfo]()
  } else {
    null
  }
  private val operatorIDQueue = if (workflowInfo != null) {
    new mutable.Queue[String]()
  } else {
    null
  }
  private val rewrittenToCacheOperatorIDs = if (null != workflowInfo) {
    new mutable.HashSet[String]()
  } else {
    null
  }

  def rewrite: WorkflowInfo = {
    if (null == workflowInfo) {
      logger.info("Rewriting workflow null")
      null
    } else {
      logger.info("Rewriting workflow {}", workflowInfo)
      checkCacheValidity()
      // Topological traversal
      workflowDAG.getSinkOperators.foreach(sinkOpID => {
        operatorIDQueue.enqueue(sinkOpID)
        newOperatorDescriptors += workflowDAG.getOperator(sinkOpID)
        addMatchingBreakpoints(sinkOpID)
      })
      while (operatorIDQueue.nonEmpty) {
        val operatorID: String = operatorIDQueue.dequeue()
        workflowDAG
          .getUpstream(operatorID)
          .foreach(upstreamDesc => {
            rewriteUpstreamOperator(operatorID, upstreamDesc)
          })
      }
      newOperatorDescriptors = newOperatorDescriptors.reverse
      removeInvalidLinks()
      WorkflowInfo(newOperatorDescriptors, newOperatorLinks, newBreakpointInfos)
    }
  }

  private def removeInvalidLinks(): Unit = {
    val opIDs = newOperatorDescriptors.map(op => op.operatorID).toSet
    val linkSets = mutable.Set[OperatorLink]()
    newOperatorLinks.foreach(link => {
      if (opIDs.contains(link.origin.operatorID) && opIDs.contains(link.destination.operatorID)) {
        linkSets += link
      }
    })
    newOperatorLinks.clear()
    linkSets.foreach(link => {
      newOperatorLinks += link
    })
  }

  private def checkCacheValidity(): Unit = {
    val sourceOperators: List[String] = workflowDAG.getSourceOperators
    sourceOperators.foreach(operatorID => {
      invalidateIfUpdated(operatorID)
      checkOperatorCacheValidity(operatorID)
    })
  }

  private def invalidateIfUpdated(operatorID: String): Unit = {
    logger.info(
      "Checking update status of operator {}.",
      workflowDAG.getOperator(operatorID).toString
    )
    if (isUpdated(operatorID)) {
      invalidateCache(operatorID)
    }
    workflowDAG
      .getDownstream(operatorID)
      .foreach(downstream => {
        invalidateIfUpdated(downstream.operatorID)
      })
  }

  private def isUpdated(opID: String): Boolean = {
    if (!operatorRecord.contains(opID)) {
      operatorRecord += ((opID, getWorkflowVertex(workflowDAG.getOperator(opID))))
      logger.info("Vertex {} is not recorded.", operatorRecord(opID))
      true
    } else {
      val vertex = getWorkflowVertex(workflowDAG.getOperator(opID))
      if (!operatorRecord(opID).equals(vertex)) {
        operatorRecord(opID) = vertex
        logger.info("Vertex {} is updated.", operatorRecord(opID))
        true
      } else {
        logger.info("Operator: {} is not updated.", operatorRecord(opID))
        false
      }
    }
  }

  private def checkOperatorCacheValidity(operatorID: String): Unit = {
    val desc = workflowDAG.getOperator(operatorID)
    logger.info("Checking cache validity of operator: {}.", desc.toString)
    if (isCacheEnabled(desc) && !isCacheValid(desc)) {
      invalidateCache(operatorID)
    }
    workflowDAG
      .getDownstream(operatorID)
      .foreach(desc => {
        checkOperatorCacheValidity(desc.operatorID)
      })
  }

  private def invalidateCache(operatorID: String): Unit = {
    operatorOutputCache.remove(operatorID)
    cachedOperatorDescriptors.remove(operatorID)
    logger.info("Operator {} cache invalidated.", operatorID)
    workflowDAG
      .getDownstream(operatorID)
      .foreach(desc => {
        invalidateCache(desc.operatorID)
      })
  }

  private def rewriteUpstreamOperator(
      operatorID: String,
      upstreamOperatorDescriptor: OperatorDescriptor
  ): Unit = {
    if (isCacheEnabled(upstreamOperatorDescriptor)) {
      if (isCacheValid(upstreamOperatorDescriptor)) {
        rewriteCachedOperator(upstreamOperatorDescriptor)
      } else {
        rewriteToCacheOperator(operatorID, upstreamOperatorDescriptor)
      }
    } else {
      rewriteNormalOperator(operatorID, upstreamOperatorDescriptor)
    }
  }

  private def rewriteNormalOperator(
      operatorID: String,
      upstreamOperatorDescriptor: OperatorDescriptor
  ): Unit = {
    // Add the new link.
    newOperatorLinks += workflowDAG.jgraphtDag.getEdge(
      upstreamOperatorDescriptor.operatorID,
      operatorID
    )
    // Remove the old link from the old DAG.
    workflowDAG.jgraphtDag.removeEdge(upstreamOperatorDescriptor.operatorID, operatorID)
    // All outgoing neighbors of this upstream operator are handled.
    if (0.equals(workflowDAG.jgraphtDag.outDegreeOf(upstreamOperatorDescriptor.operatorID))) {
      // Handle the incoming neighbors of this upstream operator.
      operatorIDQueue.enqueue(upstreamOperatorDescriptor.operatorID)
      // Add the upstream operator.
      newOperatorDescriptors += upstreamOperatorDescriptor
      // Add the old breakpoints.
      addMatchingBreakpoints(upstreamOperatorDescriptor.operatorID)
    }
  }

  private def addMatchingBreakpoints(operatorID: String): Unit = {
    workflowInfo.breakpoints.foreach(breakpoint => {
      if (operatorID.equals(breakpoint.operatorID)) {
        logger.info("Add breakpoint {} for operator {}", breakpoint, operatorID)
        newBreakpointInfos += breakpoint
      }
    })
  }

  private def rewriteToCacheOperator(
      operatorID: String,
      upstreamOperatorDescriptor: OperatorDescriptor
  ): Unit = {
    if (!rewrittenToCacheOperatorIDs.contains(upstreamOperatorDescriptor.operatorID)) {
      logger.info("Rewrite operator {}.", upstreamOperatorDescriptor.operatorID)
      val toCacheOperator = generateCacheSinkOperator(upstreamOperatorDescriptor)
      newOperatorDescriptors += toCacheOperator
      newOperatorLinks += generateToCacheLink(toCacheOperator, upstreamOperatorDescriptor)
      rewrittenToCacheOperatorIDs.add(upstreamOperatorDescriptor.operatorID)
    } else {
      logger.info("Operator {} is already rewritten.", upstreamOperatorDescriptor.operatorID)
    }
    rewriteNormalOperator(operatorID, upstreamOperatorDescriptor)
  }

  private def rewriteCachedOperator(upstreamOperatorDescriptor: OperatorDescriptor): Unit = {
    // Rewrite cached operator.
    val cacheSourceOperatorDescriptor = getCacheSourceOperator(upstreamOperatorDescriptor)
    //Add the new operator
    newOperatorDescriptors += cacheSourceOperatorDescriptor
    // Add new links.
    generateNewLinks(cacheSourceOperatorDescriptor, upstreamOperatorDescriptor).foreach(newLink => {
      newOperatorLinks += newLink
    })
    // Add new breakpoints.
    generateNewBreakpoints(cacheSourceOperatorDescriptor, upstreamOperatorDescriptor).foreach(
      newBreakpoint => {
        newBreakpointInfos += newBreakpoint
      }
    )
    // Remove the old operator and links from the old DAG.
    removeFromWorkflow(upstreamOperatorDescriptor)
  }

  private def isCacheEnabled(operatorDescriptor: OperatorDescriptor): Boolean = {
    if (!workflowInfo.cachedOperatorIDs.contains(operatorDescriptor.operatorID)) {
      operatorOutputCache.remove(operatorDescriptor.operatorID)
      cachedOperatorDescriptors.remove(operatorDescriptor.operatorID)
      logger.info("Operator {} cache not enabled.", operatorDescriptor)
      return false
    }
    logger.info("Operator {} cache enabled.", operatorDescriptor)
    true
  }

  private def isCacheValid(operatorDescriptor: OperatorDescriptor): Boolean = {
    logger.info("Checking the cache validity of operator {}.", operatorDescriptor.toString)
    assert(isCacheEnabled(operatorDescriptor))
    if (cachedOperatorDescriptors.contains(operatorDescriptor.operatorID)) {
      if (
        getCachedOperator(operatorDescriptor).equals(
          operatorDescriptor
        ) && !rewrittenToCacheOperatorIDs.contains(
          operatorDescriptor.operatorID
        )
      ) {
        logger.info("Operator {} cache valid.", operatorDescriptor)
        return true
      }
      logger.info("Operator {} cache invalid.", operatorDescriptor)
    } else {
      logger.info("cachedOperators: {}.", cachedOperatorDescriptors.toString())
      logger.info("Operator {} is never cached.", operatorDescriptor)
    }
    false
  }

  private def getCachedOperator(operatorDescriptor: OperatorDescriptor): OperatorDescriptor = {
    assert(cachedOperatorDescriptors.contains(operatorDescriptor.operatorID))
    cachedOperatorDescriptors(operatorDescriptor.operatorID)
  }

  private def generateNewLinks(
      operatorDescriptor: OperatorDescriptor,
      upstreamOperatorDescriptor: OperatorDescriptor
  ): mutable.MutableList[OperatorLink] = {
    val newOperatorLinks = mutable.MutableList[OperatorLink]()
    workflowDAG.jgraphtDag
      .outgoingEdgesOf(upstreamOperatorDescriptor.operatorID)
      .forEach(link => {
        val originOperatorPort =
          OperatorPort(operatorDescriptor.operatorID, link.origin.portOrdinal)
        val newOperatorLink = OperatorLink(originOperatorPort, link.destination)
        newOperatorLinks += newOperatorLink
      })
    newOperatorLinks
  }

  private def generateNewBreakpoints(
      newOperatorDescriptor: OperatorDescriptor,
      upstreamOperatorDescriptor: OperatorDescriptor
  ): mutable.MutableList[BreakpointInfo] = {
    val breakpointInfoList = new mutable.MutableList[BreakpointInfo]()
    workflowInfo.breakpoints.foreach(info => {
      if (upstreamOperatorDescriptor.operatorID.equals(info.operatorID)) {
        breakpointInfoList += BreakpointInfo(newOperatorDescriptor.operatorID, info.breakpoint)
      }
    })
    breakpointInfoList
  }

  private def removeFromWorkflow(operator: OperatorDescriptor): Unit = {
    workflowDAG.jgraphtDag.removeVertex(operator.operatorID)
  }

  private def generateCacheSinkOperator(operatorDescriptor: OperatorDescriptor): CacheSinkOpDesc = {
    logger.info("Generating CacheSinkOperator for operator {}.", operatorDescriptor.toString)
    val outputTupleCache = mutable.MutableList[Tuple]()
    cachedOperatorDescriptors += ((operatorDescriptor.operatorID, copyOperator(operatorDescriptor)))
    logger.info(
      "Operator: {} added to cachedOperators: {}.",
      operatorDescriptor.toString,
      cachedOperatorDescriptors.toString()
    )
//    logger.info("cachedOperators size: {}.", cachedOperatorDescriptors.size)
    val cacheSinkOperator = new CacheSinkOpDesc(outputTupleCache)
    cacheSinkOperatorDescriptors += ((operatorDescriptor.operatorID, cacheSinkOperator))
    val cacheSourceOperator = new CacheSourceOpDesc(outputTupleCache)
    cacheSourceOperatorDescriptors += ((operatorDescriptor.operatorID, cacheSourceOperator))
    cacheSinkOperator
  }

  private def getCacheSourceOperator(operatorDescriptor: OperatorDescriptor): CacheSourceOpDesc = {
    val cacheSourceOperator = cacheSourceOperatorDescriptors(operatorDescriptor.operatorID)
    cacheSourceOperator.schema = cacheSinkOperatorDescriptors(operatorDescriptor.operatorID).schema
    cacheSourceOperator
  }

  private def generateToCacheLink(
      destinationOperatorDescriptor: OperatorDescriptor,
      originOperatorDescriptor: OperatorDescriptor
  ): OperatorLink = {
    //TODO: How to set the port ordinal?
    val originOperatorPort: OperatorPort = OperatorPort(originOperatorDescriptor.operatorID, 0)
    val destinationOperatorPort: OperatorPort =
      OperatorPort(destinationOperatorDescriptor.operatorID, 0)
    OperatorLink(originOperatorPort, destinationOperatorPort)
  }

  def getWorkflowVertex(op: OperatorDescriptor): WorkflowVertex = {
    val opInVertex = copyOperator(op)
    val links = mutable.HashSet[OperatorLink]()
    if (!workflowDAG.operators.contains(op.operatorID)) {
      null
    } else {
      workflowDAG.jgraphtDag.outgoingEdgesOf(opInVertex.operatorID).forEach(links.+=)
      workflowDAG.jgraphtDag.incomingEdgesOf(opInVertex.operatorID).forEach(links.+=)
      val breakpoints = mutable.HashSet[BreakpointInfo]()
      workflowInfo.breakpoints
        .filter(breakpoint => breakpoint.operatorID.equals(op.operatorID))
        .foreach(breakpoints.+=)
      new WorkflowVertex(opInVertex, links, breakpoints)
    }
  }
}
