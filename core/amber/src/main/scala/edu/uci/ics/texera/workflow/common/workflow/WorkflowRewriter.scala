package edu.uci.ics.texera.workflow.common.workflow

import com.typesafe.scalalogging.Logger
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowRewriter.operatorDescToString
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc

import scala.collection.mutable

object WorkflowRewriter {
  private def operatorDescToString(operatorDesc: OperatorDescriptor): String = {
    var str: String = operatorDesc.toString
    val start = str.indexOf('[')
    str = str.substring(start + 1, str.length - 1)
    str
  }
}

//TODO: Use WorkflowResultService.
class WorkflowRewriter(
    var workflowInfo: WorkflowInfo,
    var operatorOutputCache: mutable.HashMap[String, mutable.MutableList[Tuple]],
    var cachedOperatorIDs: mutable.HashMap[String, String],
    var cacheSourceOperatorDescriptors: mutable.HashMap[String, CacheSourceOpDesc],
    var cacheSinkOperatorDescriptors: mutable.HashMap[String, CacheSinkOpDesc]
) {
  private val logger = Logger(this.getClass.getName)

  var operatorRecord: mutable.HashMap[String, String] = _

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
      WorkflowInfo(newOperatorDescriptors, newOperatorLinks, newBreakpointInfos)
    }
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

  private def isUpdated(operatorID: String): Boolean = {
    if (!operatorRecord.contains(operatorID)) {
      val str = operatorIDToString(operatorID)
      operatorRecord += ((operatorID, str))
      logger.info("Operator: {} is not recorded.", workflowDAG.getOperator(operatorID).toString)
      true
    } else {
      val str = operatorIDToString(operatorID)
      if (!operatorRecord(operatorID).equals(str)) {
        operatorRecord(operatorID) = str
        logger.info("Operator: {} is updated.", workflowDAG.getOperator(operatorID).toString)
        true
      } else {
        logger.info("Operator: {} is not updated.", workflowDAG.getOperator(operatorID).toString)
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
    cachedOperatorIDs.remove(operatorID)
    logger.info("Operator {} cache invalidated.", operatorID)
    workflowDAG
      .getDownstream(operatorID)
      .foreach(desc => {
        invalidateCache(desc.operatorID)
      })
  }

  private def rewriteUpstreamOperator(operatorID: String, upstreamOp: OperatorDescriptor): Unit = {
    if (isCacheEnabled(upstreamOp)) {
      if (isCacheValid(upstreamOp)) {
        rewriteCachedOperator(upstreamOp)
      } else {
        rewriteToCacheOperator(operatorID, upstreamOp)
      }
    } else {
      rewriteNormalOperator(operatorID, upstreamOp)
    }
  }

  private def rewriteNormalOperator(opID: String, upstreamOp: OperatorDescriptor): Unit = {
    // Add the new link.
    newOperatorLinks += workflowDAG.jgraphtDag.getEdge(upstreamOp.operatorID, opID)
    // Remove the old link from the old DAG.
    workflowDAG.jgraphtDag.removeEdge(upstreamOp.operatorID, opID)
    // All outgoing neighbors of this upstream operator are handled.
    if (0.equals(workflowDAG.jgraphtDag.outDegreeOf(upstreamOp.operatorID))) {
      // Handle the incoming neighbors of this upstream operator.
      operatorIDQueue.enqueue(upstreamOp.operatorID)
      // Add the upstream operator.
      newOperatorDescriptors += upstreamOp
      // Add the old breakpoints.
      addMatchingBreakpoints(upstreamOp.operatorID)
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

  private def rewriteToCacheOperator(opID: String, upstreamOp: OperatorDescriptor): Unit = {
    if (!rewrittenToCacheOperatorIDs.contains(upstreamOp.operatorID)) {
      logger.info("Rewrite operator {}.", upstreamOp.operatorID)
      val toCacheOperator = generateCacheSinkOperator(upstreamOp)
      newOperatorDescriptors += toCacheOperator
      newOperatorLinks += generateToCacheLink(toCacheOperator, upstreamOp)
      rewrittenToCacheOperatorIDs.add(upstreamOp.operatorID)
    } else {
      logger.info("Operator {} is already rewritten.", upstreamOp.operatorID)
    }
    rewriteNormalOperator(opID, upstreamOp)
  }

  private def rewriteCachedOperator(upstreamOp: OperatorDescriptor): Unit = {
    // Rewrite cached operator.
    val cachedOperator = getCacheSourceOperator(upstreamOp)
    //Add the new operator
    newOperatorDescriptors += cachedOperator
    // Add new links.
    generateNewLinks(cachedOperator, upstreamOp).foreach(newLink => {
      newOperatorLinks += newLink
    })
    // Add new breakpoints.
    generateNewBreakpoints(cachedOperator, upstreamOp).foreach(newBreakpoint => {
      newBreakpointInfos += newBreakpoint
    })
    // Remove the old operator and links from the old DAG.
    removeFromWorkflow(upstreamOp)
  }

  private def isCacheEnabled(operator: OperatorDescriptor): Boolean = {
    if (!workflowInfo.cachedOperatorIDs.contains(operator.operatorID)) {
      operatorOutputCache.remove(operator.operatorID)
      cachedOperatorIDs.remove(operator.operatorID)
      logger.info("Operator {} cache not enabled.", operator)
      return false
    }
    logger.info("Operator {} cache enabled.", operator)
    true
  }

  private def isCacheValid(operator: OperatorDescriptor): Boolean = {
    logger.info("Checking the cache validity of operator {}.", operator.toString)
    assert(isCacheEnabled(operator))
    if (cachedOperatorIDs.contains(operator.operatorID)) {
      if (
        getCachedOperator(operator).equals(
          operatorIDToString(operator.operatorID)
        ) && !rewrittenToCacheOperatorIDs.contains(
          operator.operatorID
        )
      ) {
        logger.info("Operator {} cache valid.", operator)
        return true
      }
      logger.info("Operator {} cache invalid.", operator)
    } else {
      logger.info("cachedOperators: {}.", cachedOperatorIDs.toString())
      logger.info("Operator {} is never cached.", operator)
    }
    false
  }

  private def getCachedOperator(operator: OperatorDescriptor): String = {
    assert(cachedOperatorIDs.contains(operator.operatorID))
    cachedOperatorIDs(operator.operatorID)
  }

  private def generateNewLinks(
      operator: OperatorDescriptor,
      upstreamOp: OperatorDescriptor
  ): mutable.MutableList[OperatorLink] = {
    val newLinks = mutable.MutableList[OperatorLink]()
    workflowDAG.jgraphtDag
      .outgoingEdgesOf(upstreamOp.operatorID)
      .forEach(link => {
        val origin = OperatorPort(operator.operatorID, link.origin.portOrdinal)
        val newLink = OperatorLink(origin, link.destination)
        newLinks += newLink
      })
    newLinks
  }

  private def generateNewBreakpoints(
      newOperator: OperatorDescriptor,
      upstreamOp: OperatorDescriptor
  ): mutable.MutableList[BreakpointInfo] = {
    val breakpointInfoList = new mutable.MutableList[BreakpointInfo]()
    workflowInfo.breakpoints.foreach(info => {
      if (upstreamOp.operatorID.equals(info.operatorID)) {
        breakpointInfoList += BreakpointInfo(newOperator.operatorID, info.breakpoint)
      }
    })
    breakpointInfoList
  }

  private def removeFromWorkflow(operator: OperatorDescriptor): Unit = {
    workflowDAG.jgraphtDag.removeVertex(operator.operatorID)
  }

  private def generateCacheSinkOperator(operator: OperatorDescriptor): CacheSinkOpDesc = {
    logger.info("Generating CacheSinkOperator for operator {}.", operator.toString)
    val outputCache = mutable.MutableList[Tuple]()
    cachedOperatorIDs += ((operator.operatorID, operatorIDToString(operator.operatorID)))
    logger.info(
      "Operator: {} added to cachedOperators: {}.",
      operator.toString,
      cachedOperatorIDs.toString()
    )
    logger.info("cachedOperators size: {}.", cachedOperatorIDs.size)
    val cacheSinkOperator = new CacheSinkOpDesc(outputCache)
    cacheSinkOperatorDescriptors += ((operator.operatorID, cacheSinkOperator))
    val cacheSourceOperator = new CacheSourceOpDesc(outputCache)
    cacheSourceOperatorDescriptors += ((operator.operatorID, cacheSourceOperator))
    cacheSinkOperator
  }

  private def getCacheSourceOperator(operatorDesc: OperatorDescriptor): CacheSourceOpDesc = {
    val cacheSourceOperator = cacheSourceOperatorDescriptors(operatorDesc.operatorID)
    cacheSourceOperator.schema = cacheSinkOperatorDescriptors(operatorDesc.operatorID).schema
    cacheSourceOperator
  }

  private def generateToCacheLink(
      destinationDesc: OperatorDescriptor,
      originDesc: OperatorDescriptor
  ): OperatorLink = {
    //TODO: How to set the port ordinal?
    val originPort: OperatorPort = OperatorPort(originDesc.operatorID, 0)
    val destinationPort: OperatorPort = OperatorPort(destinationDesc.operatorID, 0)
    OperatorLink(originPort, destinationPort)
  }

  private def operatorIDToString(operatorID: String): String = {
    operatorDescToString(workflowDAG.getOperator(operatorID))
  }
}
