package edu.uci.ics.texera.workflow.common.workflow

import com.typesafe.scalalogging.Logger
import edu.uci.ics.amber.engine.architecture.storage.OpResultStorage
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.WorkflowRewriter.copyOperator
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDescV2
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc

import java.util.UUID
import scala.collection.mutable

case class WorkflowVertex(
    op: OperatorDescriptor,
    links: mutable.HashSet[OperatorLink]
)

object WorkflowRewriter {
  private def copyOperator(operator: OperatorDescriptor): OperatorDescriptor = {
    objectMapper.readValue(objectMapper.writeValueAsString(operator), classOf[OperatorDescriptor])
  }
}

class WorkflowRewriter(
                        var workflowInfo: WorkflowInfo,
                        var cachedOperatorDescriptors: mutable.HashMap[String, OperatorDescriptor],
                        var cacheSourceOperatorDescriptors: mutable.HashMap[String, CacheSourceOpDesc],
                        var cacheSinkOperatorDescriptors: mutable.HashMap[String, CacheSinkOpDescV2],
                        var operatorRecord: mutable.HashMap[String, WorkflowVertex],
                        var opResultStorage: OpResultStorage
) {

  private val logger = Logger(this.getClass.getName)

  var visitedOpID: mutable.HashSet[String] = new mutable.HashSet[String]()

  private val workflowDAG: WorkflowDAG = if (workflowInfo != null) {
    new WorkflowDAG(workflowInfo)
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

  def rewrite: WorkflowInfo = {
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
      val cacheSinkOp = generateCacheSinkOperator_v2(op)
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

  def cacheStatusUpdate(): mutable.Set[String] = {

    val invalidSet = mutable.HashSet[String]()

    def invalidateOperatorCache(id: String): Unit = {
      if (cachedOperatorDescriptors.contains(id)) {
        cachedOperatorDescriptors.remove(id)
        cacheSinkOperatorDescriptors.remove(id)
        cacheSourceOperatorDescriptors.remove(id)
        invalidSet += id
      }
      logger.info("Operator {} cache invalidated.", id)
    }

    def invalidateOperatorCacheRecursively(opID: String): Unit = {
      invalidateOperatorCache(opID)
      workflowDAG
        .getDownstream(opID)
        .foreach(desc => {
          invalidateOperatorCacheRecursively(desc.operatorID)
        })
    }

    def isUpdated(opID: String): Boolean = {
      if (!operatorRecord.contains(opID)) {
        operatorRecord += ((opID, getWorkflowVertex(workflowDAG.getOperator(opID))))
        logger.info("Vertex {} is not recorded.", operatorRecord(opID))
        true
      } else if (workflowInfo.cachedOperatorIDs.contains(opID)) {
        !operatorRecord(opID).equals(getWorkflowVertex(workflowDAG.getOperator(opID)))
      } else {
        val vertex = getWorkflowVertex(workflowDAG.getOperator(opID))
        if (!operatorRecord(opID).equals(vertex)) {
          operatorRecord(opID) = vertex
          logger.info("Vertex {} is updated.", operatorRecord(opID))
          true
        } else if (cachedOperatorDescriptors.contains(opID)) {
          !workflowInfo.cachedOperatorIDs.contains(opID)
        } else {
          logger.info("Operator: {} is not updated.", operatorRecord(opID))
          false
        }
      }
    }

    def cacheStatusUpdateHelper(opID: String): Unit = {
      if (visitedOpID.contains(opID)) {
        return
      }
      visitedOpID += opID
      logger.info(
        "Checking update status of operator {}.",
        workflowDAG.getOperator(opID).toString
      )
      if (isUpdated(opID)) {
        invalidateOperatorCache(opID)
        workflowDAG
          .getDownstream(opID)
          .map(op => op.operatorID)
          .foreach(invalidateOperatorCacheRecursively)
      }
    }

    val opIter = workflowDAG.jgraphtDag.iterator()
    while (opIter.hasNext) {
      val opID = opIter.next()
      if (!visitedOpID.contains(opID)) {
        cacheStatusUpdateHelper(opID)
      }
    }
    workflowInfo.operators
      .map(op => op.operatorID)
      .filterNot(visitedOpID.contains)
      .foreach(invalidSet.+=)
    invalidSet
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

  def isUpdated(id: String): Boolean = {
    if (!operatorRecord.contains(id)) {
      operatorRecord += ((id, getWorkflowVertex(workflowDAG.getOperator(id))))
      logger.info("Vertex {} is not recorded.", operatorRecord(id))
      true
    } else if (workflowInfo.cachedOperatorIDs.contains(id)) {
      if (cachedOperatorDescriptors.contains(id)) {
        val vertex = getWorkflowVertex(workflowDAG.getOperator(id))
        if (operatorRecord(id).equals(vertex)) {
          false
        } else {
          operatorRecord(id) = vertex
          true
        }
      } else {
        true
      }
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
    if (cachedOperatorDescriptors.contains(id)) {
      cachedOperatorDescriptors.remove(id)
      opResultStorage.remove(cacheSinkOperatorDescriptors(id).uuid)
      cacheSinkOperatorDescriptors.remove(id)
      cacheSourceOperatorDescriptors.remove(id)
    }
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

  private def isCacheEnabled(operatorDescriptor: OperatorDescriptor): Boolean = {
    if (!workflowInfo.cachedOperatorIDs.contains(operatorDescriptor.operatorID)) {
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

  private def generateCacheSinkOperator_v2(
      operatorDescriptor: OperatorDescriptor
  ): CacheSinkOpDescV2 = {
    logger.info("Generating CacheSinkOperator for operator {}.", operatorDescriptor.toString)
    cachedOperatorDescriptors += ((operatorDescriptor.operatorID, copyOperator(operatorDescriptor)))
    logger.info(
      "Operator: {} added to cachedOperators: {}.",
      operatorDescriptor.toString,
      cachedOperatorDescriptors.toString()
    )
    val uuid = UUID.randomUUID().toString
    val cacheSinkOperator = new CacheSinkOpDescV2(uuid, opResultStorage)
    cacheSinkOperatorDescriptors += ((operatorDescriptor.operatorID, cacheSinkOperator))
    val cacheSourceOperator = new CacheSourceOpDesc(uuid, opResultStorage)
    cacheSourceOperatorDescriptors += ((operatorDescriptor.operatorID, cacheSourceOperator))
    cacheSinkOperator
  }

  private def getCacheSourceOperator(
      operatorDescriptor: OperatorDescriptor
  ): CacheSourceOpDesc = {
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
      workflowDAG.jgraphtDag.incomingEdgesOf(opInVertex.operatorID).forEach(link => links.+=(link))
      WorkflowVertex(opInVertex, links)
    }
  }

}
