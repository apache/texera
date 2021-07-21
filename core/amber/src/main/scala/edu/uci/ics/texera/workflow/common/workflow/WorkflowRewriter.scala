package edu.uci.ics.texera.workflow.common.workflow

import com.typesafe.scalalogging.Logger
import edu.uci.ics.texera.workflow.common.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.WorkflowRewriter.copyOperator
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc

import scala.collection.mutable

case class WorkflowVertex(
    op: OperatorDescriptor,
    links: mutable.HashSet[OperatorLink],
    breakpoints: mutable.HashSet[BreakpointInfo]
)

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
    var cacheSinkOperatorDescriptors: mutable.HashMap[String, CacheSinkOpDesc],
    var operatorRecord: mutable.HashMap[String, WorkflowVertex]
) {
  private val logger = Logger(this.getClass.getName)

  var visitedOpID: mutable.HashSet[String] = new mutable.HashSet[String]()

  private val workflowDAG: WorkflowDAG = if (workflowInfo != null) {
    new WorkflowDAG(workflowInfo)
  } else {
    null
  }
  private var newOps = if (workflowInfo != null) {
    mutable.MutableList[OperatorDescriptor]()
  } else {
    null
  }
  private val newLinks = if (workflowInfo != null) {
    mutable.MutableList[OperatorLink]()
  } else {
    null
  }
  private val newBreakpoints = if (workflowInfo != null) {
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
  private val r1NewOps = if (workflowInfo != null) {
    mutable.MutableList[OperatorDescriptor]()
  } else {
    null
  }
  private val r1NewLinks = if (workflowInfo != null) {
    mutable.MutableList[OperatorLink]()
  } else {
    null
  }
  private val r1NewBreakpoints = if (workflowInfo != null) {
    mutable.MutableList[BreakpointInfo]()
  } else {
    null
  }
  private val r2NewOps = if (workflowInfo != null) {
    mutable.MutableList[OperatorDescriptor]()
  } else {
    null
  }
  private val r2NewLinks = if (workflowInfo != null) {
    mutable.MutableList[OperatorLink]()
  } else {
    null
  }
  private val r2NewBreakpoints = if (workflowInfo != null) {
    mutable.MutableList[BreakpointInfo]()
  } else {
    null
  }
  private val r2OpIDQue = if (workflowInfo != null) {
    new mutable.Queue[String]()
  } else {
    null
  }

  var r1WorkflowInfo: WorkflowInfo = _
  var r1WorkflowDAG: WorkflowDAG = _
  var r2WorkflowInfo: WorkflowInfo = _
  var r2WorkflowDAG: WorkflowDAG = _

  def rewrite_v2: WorkflowInfo = {
    if (null == workflowInfo) {
      logger.info("Rewriting workflow null")
      null
    } else {
      logger.info("Rewriting workflow {}", workflowInfo)
      checkCacheValidity()

      workflowDAG.getSinkOperators.foreach(r2OpIDQue.+=)
      while (r2OpIDQue.nonEmpty) {
        r2(r2OpIDQue.dequeue())
      }
      val r2TmpLinks = mutable.MutableList[OperatorLink]()
      val r2TmpOpIDs = r2NewOps.map(op => op.operatorID)
      r2NewLinks.foreach(link => {
        if (r2TmpOpIDs.contains(link.destination.operatorID)) {
          if (r2TmpOpIDs.contains(link.origin.operatorID)) {
            r2TmpLinks += link
          }
        }
      })
      r2NewLinks.clear()
      r2TmpLinks.foreach(r2NewLinks.+=)

      r2WorkflowInfo = WorkflowInfo(r2NewOps, r2NewLinks, r2NewBreakpoints)
      r2WorkflowDAG = new WorkflowDAG(r2WorkflowInfo)
      r2WorkflowDAG.getSinkOperators.foreach(r2OpIDQue.+=)

      val r1OpIDIter = r2WorkflowDAG.jgraphtDag.iterator()
      var r1OpIDs: mutable.MutableList[String] = mutable.MutableList[String]()
      r1OpIDIter.forEachRemaining(id => r1OpIDs.+=(id))
      r1OpIDs = r1OpIDs.reverse
      r1OpIDs.foreach(r1)

      new WorkflowInfo(r1NewOps, r1NewLinks, r1NewBreakpoints)
    }
  }

  private def r1(opID: String): Unit = {
    val op = r2WorkflowDAG.getOperator(opID)
    if (isCacheEnabled(op) && !isCacheValid(op)) {
      val cacheSinkOp = generateCacheSinkOperator(op)
      val cacheSinkLink = generateCacheSinkLink(cacheSinkOp, op)
      r1NewOps += cacheSinkOp
      r1NewLinks += cacheSinkLink
    }
    r1NewOps += op
    r2WorkflowDAG.jgraphtDag
      .outgoingEdgesOf(opID)
      .forEach(link => {
        r1NewLinks += link
      })
    r2WorkflowInfo.breakpoints.foreach(breakpoint => {
      if (breakpoint.operatorID.equals(opID)) {
        r1NewBreakpoints += breakpoint
      }
    })
  }

  private def r2(opID: String): Unit = {
    val op = workflowDAG.getOperator(opID)
    if (isCacheEnabled(op) && isCacheValid(op)) {
      val cacheSourceOp = getCacheSourceOperator(op)
      r2NewOps += cacheSourceOp
      workflowDAG.jgraphtDag
        .outgoingEdgesOf(opID)
        .forEach(link => {
          val src = OperatorPort(cacheSourceOp.operatorID, link.origin.portOrdinal)
          val dest = link.destination
          r2NewLinks += OperatorLink(src, dest)
        })
      workflowInfo.breakpoints.foreach(breakpoint => {
        if (breakpoint.operatorID.equals(opID)) {
          r2NewBreakpoints += BreakpointInfo(cacheSourceOp.operatorID, breakpoint.breakpoint)
        }
      })
    } else {
      r2NewOps += op
      workflowDAG.jgraphtDag.outgoingEdgesOf(opID).forEach(link => r2NewLinks.+=(link))
      workflowInfo.breakpoints.foreach(breakpoint => {
        if (breakpoint.operatorID.equals(op.operatorID)) {
          r2NewBreakpoints += breakpoint
        }
      })
      workflowDAG
        .getUpstream(opID)
        .map(_.operatorID)
        .foreach(id => {
          if (!r2OpIDQue.contains(id)) {
            r2OpIDQue += id
          }
        })
    }
  }

  private def removeInvalidLinks(): Unit = {
    val opIDs = newOps.map(op => op.operatorID).toSet
    val linkSets = mutable.Set[OperatorLink]()
    newLinks.foreach(link => {
      if (opIDs.contains(link.origin.operatorID) && opIDs.contains(link.destination.operatorID)) {
        linkSets += link
      }
    })
    newLinks.clear()
    linkSets.foreach(link => {
      newLinks += link
    })
  }

  private def checkCacheValidity(): Unit = {
    val opIter = workflowDAG.jgraphtDag.iterator()
    while (opIter.hasNext) {
      val id = opIter.next()
      if (!visitedOpID.contains(id)) {
        invalidateIfUpdated(id)
      }
    }
  }

  private def invalidateIfUpdated(operatorID: String): Unit = {
    if (visitedOpID.contains(operatorID)) {
      return
    }
    visitedOpID += operatorID
    logger.info(
      "Checking update status of operator {}.",
      workflowDAG.getOperator(operatorID).toString
    )
    if (isUpdated(operatorID)) {
      invalidateOperatorCache(operatorID)
      workflowDAG
        .getDownstream(operatorID)
        .map(op => op.operatorID)
        .foreach(invalidateOperatorCacheRecursively)
    }
  }

  private def isUpdated(id: String): Boolean = {
    if (!operatorRecord.contains(id)) {
      operatorRecord += ((id, getWorkflowVertex(workflowDAG.getOperator(id))))
      logger.info("Vertex {} is not recorded.", operatorRecord(id))
      true
    } else {
      val vertex = getWorkflowVertex(workflowDAG.getOperator(id))
      if (!operatorRecord(id).equals(vertex)) {
        operatorRecord(id) = vertex
        logger.info("Vertex {} is updated.", operatorRecord(id))
        true
      } else if (cachedOperatorDescriptors.contains(id)) {
        !workflowInfo.cachedOperatorIDs.contains(id)
      } else {
        logger.info("Operator: {} is not updated.", operatorRecord(id))
        false
      }
    }
  }

  private def invalidateOperatorCache(id: String): Unit = {
    operatorOutputCache.remove(id)
    cachedOperatorDescriptors.remove(id)
    logger.info("Operator {} cache invalidated.", id)
  }

  private def invalidateOperatorCacheRecursively(operatorID: String): Unit = {
    invalidateOperatorCache(operatorID)
    workflowDAG
      .getDownstream(operatorID)
      .foreach(desc => {
        invalidateOperatorCacheRecursively(desc.operatorID)
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
    newLinks += workflowDAG.jgraphtDag.getEdge(
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
      newOps += upstreamOperatorDescriptor
      // Add the old breakpoints.
      addMatchingBreakpoints(upstreamOperatorDescriptor.operatorID)
    }
  }

  private def addMatchingBreakpoints(operatorID: String): Unit = {
    workflowInfo.breakpoints.foreach(breakpoint => {
      if (operatorID.equals(breakpoint.operatorID)) {
        logger.info("Add breakpoint {} for operator {}", breakpoint, operatorID)
        newBreakpoints += breakpoint
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
      newOps += toCacheOperator
      newLinks += generateCacheSinkLink(toCacheOperator, upstreamOperatorDescriptor)
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
    newOps += cacheSourceOperatorDescriptor
    // Add new links.
    generateNewLinks(cacheSourceOperatorDescriptor, upstreamOperatorDescriptor).foreach(newLink => {
      newLinks += newLink
    })
    // Add new breakpoints.
    generateNewBreakpoints(cacheSourceOperatorDescriptor, upstreamOperatorDescriptor).foreach(
      newBreakpoint => {
        newBreakpoints += newBreakpoint
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

  private def generateCacheSinkLink(
      dest: OperatorDescriptor,
      src: OperatorDescriptor
  ): OperatorLink = {
    //TODO: How to set the port ordinal?
    val destPort: OperatorPort = OperatorPort(dest.operatorID, 0)
    val srcPort: OperatorPort = OperatorPort(src.operatorID, 0)
    OperatorLink(srcPort, destPort)
  }

  def getWorkflowVertex(op: OperatorDescriptor): WorkflowVertex = {
    val opInVertex = copyOperator(op)
    val links = mutable.HashSet[OperatorLink]()
    if (!workflowDAG.operators.contains(op.operatorID)) {
      null
    } else {
      //      workflowDAG.jgraphtDag.outgoingEdgesOf(opInVertex.operatorID).forEach(links.+=)
      workflowDAG.jgraphtDag.incomingEdgesOf(opInVertex.operatorID).forEach(link => links.+=(link))
      val breakpoints = mutable.HashSet[BreakpointInfo]()
      workflowInfo.breakpoints
        .filter(breakpoint => breakpoint.operatorID.equals(op.operatorID))
        .foreach(breakpoints.+=)
      WorkflowVertex(opInVertex, links, breakpoints)
    }
  }

  @Deprecated
  def rewrite: WorkflowInfo = {
    if (null == workflowInfo) {
      throw new Exception("Rewriting null workflow.")
      null
    } else {
      logger.info("Rewriting workflow {}", workflowInfo)
      checkCacheValidity()
      // Topological traversal
      workflowDAG.getSinkOperators.foreach(sinkOpID => {
        operatorIDQueue.enqueue(sinkOpID)
        newOps += workflowDAG.getOperator(sinkOpID)
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
      newOps = newOps.reverse
      removeInvalidLinks()
      WorkflowInfo(newOps, newLinks, newBreakpoints)
    }
  }

  @Deprecated
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

  @Deprecated
  private def invalidateCache(operatorID: String): Unit = {
    invalidateOperatorCache(operatorID)
    workflowDAG
      .getDownstream(operatorID)
      .foreach(desc => {
        invalidateCache(desc.operatorID)
      })
  }
}
