package edu.uci.ics.texera.workflow.common.workflow

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.WorkflowRewriter.copyOperator
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import java.util.UUID

import edu.uci.ics.texera.workflow.operators.sink.managed.{
  ProgressiveSinkOpDesc,
  ProgressiveSinkOpExec
}

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
    val workflowInfo: WorkflowInfo,
    val cachedOperatorDescriptors: mutable.HashMap[String, OperatorDescriptor],
    val cacheSourceOperatorDescriptors: mutable.HashMap[String, CacheSourceOpDesc],
    val cacheSinkOperatorDescriptors: mutable.HashMap[String, ProgressiveSinkOpDesc],
    val operatorRecord: mutable.HashMap[String, WorkflowVertex],
    val opResultStorage: OpResultStorage
) extends LazyLogging {

  var visitedOpIdSet: mutable.HashSet[String] = new mutable.HashSet[String]()

  private def workflowDAG: workflowInfo.WorkflowDAG = workflowInfo.toDAG

  private val rewrittenToCacheOperatorIDs = if (null != workflowInfo) {
    new mutable.HashSet[String]()
  } else {
    null
  }
  private val addCacheSinkNewOps = if (workflowInfo != null) {
    mutable.MutableList[OperatorDescriptor]()
  } else {
    null
  }
  private val addCacheSinkNewLinks = if (workflowInfo != null) {
    mutable.MutableList[OperatorLink]()
  } else {
    null
  }
  private val addCacheSinkNewBreakpoints = if (workflowInfo != null) {
    mutable.MutableList[BreakpointInfo]()
  } else {
    null
  }
  private val addCacheSourceNewOps = if (workflowInfo != null) {
    mutable.MutableList[OperatorDescriptor]()
  } else {
    null
  }
  private val addCacheSourceNewLinks = if (workflowInfo != null) {
    mutable.MutableList[OperatorLink]()
  } else {
    null
  }
  private val addCacheSourceNewBreakpoints = if (workflowInfo != null) {
    mutable.MutableList[BreakpointInfo]()
  } else {
    null
  }
  private val addCacheSourceOpIdQue = if (workflowInfo != null) {
    new mutable.Queue[String]()
  } else {
    null
  }

  var addCacheSourceWorkflowInfo: WorkflowInfo = _

  def rewrite: WorkflowInfo = {
    if (null == workflowInfo) {
      logger.debug("Rewriting workflow null")
      null
    } else {
      logger.info("Rewriting workflow {}", workflowInfo)
      checkCacheValidity()

      workflowDAG.getSinkOperators.foreach(addCacheSourceOpIdQue.+=)

      // Topological traverse and add cache source operators.
      while (addCacheSourceOpIdQue.nonEmpty) {
        addCacheSource(addCacheSourceOpIdQue.dequeue())
      }

      // Collect the output of adding cache source.
      val addCacheSourceTmpLinks = mutable.MutableList[OperatorLink]()
      val addCacheSourceTmpOpIds = addCacheSourceNewOps.map(op => op.operatorID)
      addCacheSourceNewLinks.foreach(link => {
        if (addCacheSourceTmpOpIds.contains(link.destination.operatorID)) {
          if (addCacheSourceTmpOpIds.contains(link.origin.operatorID)) {
            addCacheSourceTmpLinks += link
          }
        }
      })
      addCacheSourceNewLinks.clear()
      addCacheSourceTmpLinks.foreach(addCacheSourceNewLinks.+=)

      addCacheSourceWorkflowInfo =
        WorkflowInfo(addCacheSourceNewOps, addCacheSourceNewLinks, addCacheSourceNewBreakpoints)
      addCacheSourceWorkflowInfo.toDAG.getSinkOperators.foreach(addCacheSourceOpIdQue.+=)

      // Topological traverse and add cache sink operators.
      val addCacheSinkOpIdIter = addCacheSourceWorkflowInfo.toDAG.jgraphtDag.iterator()
      var addCacheSinkOpIds: mutable.MutableList[String] = mutable.MutableList[String]()
      addCacheSinkOpIdIter.forEachRemaining(opId => addCacheSinkOpIds.+=(opId))
      addCacheSinkOpIds = addCacheSinkOpIds.reverse
      addCacheSinkOpIds.foreach(addCacheSink)

      new WorkflowInfo(addCacheSinkNewOps, addCacheSinkNewLinks, addCacheSinkNewBreakpoints)
    }
  }

  private def addCacheSink(opId: String): Unit = {
    val op = addCacheSourceWorkflowInfo.toDAG.getOperator(opId)
    if (isCacheEnabled(op) && !isCacheValid(op)) {
      val cacheSinkOp = generateCacheSinkOperator(op)
      val cacheSinkLink = generateCacheSinkLink(cacheSinkOp, op)
      addCacheSinkNewOps += cacheSinkOp
      addCacheSinkNewLinks += cacheSinkLink
    }
    addCacheSinkNewOps += op
    addCacheSourceWorkflowInfo.toDAG.jgraphtDag
      .outgoingEdgesOf(opId)
      .forEach(link => {
        addCacheSinkNewLinks += link
      })
    addCacheSourceWorkflowInfo.breakpoints.foreach(breakpoint => {
      if (breakpoint.operatorID.equals(opId)) {
        addCacheSinkNewBreakpoints += breakpoint
      }
    })
  }

  private def addCacheSource(opId: String): Unit = {
    val op = workflowDAG.getOperator(opId)
    if (isCacheEnabled(op) && isCacheValid(op)) {
      val cacheSourceOp = getCacheSourceOperator(op)
      addCacheSourceNewOps += cacheSourceOp
      workflowDAG.jgraphtDag
        .outgoingEdgesOf(opId)
        .forEach(link => {
          val src = OperatorPort(cacheSourceOp.operatorID, link.origin.portOrdinal)
          val dest = link.destination
          addCacheSourceNewLinks += OperatorLink(src, dest)
        })
      workflowInfo.breakpoints.foreach(breakpoint => {
        if (breakpoint.operatorID.equals(opId)) {
          addCacheSourceNewBreakpoints += BreakpointInfo(
            cacheSourceOp.operatorID,
            breakpoint.breakpoint
          )
        }
      })
    } else {
      addCacheSourceNewOps += op
      workflowDAG.jgraphtDag.outgoingEdgesOf(opId).forEach(link => addCacheSourceNewLinks.+=(link))
      workflowInfo.breakpoints.foreach(breakpoint => {
        if (breakpoint.operatorID.equals(op.operatorID)) {
          addCacheSourceNewBreakpoints += breakpoint
        }
      })
      workflowDAG
        .getUpstream(opId)
        .map(_.operatorID)
        .foreach(opId => {
          if (!addCacheSourceOpIdQue.contains(opId)) {
            addCacheSourceOpIdQue += opId
          }
        })
    }
  }

  def cacheStatusUpdate(): mutable.Set[String] = {

    val invalidSet = mutable.HashSet[String]()

    def invalidateOperatorCacheForCacheStatusUpdate(opId: String): Unit = {
      if (cachedOperatorDescriptors.contains(opId)) {
        cachedOperatorDescriptors.remove(opId)
        cacheSinkOperatorDescriptors.remove(opId)
        cacheSourceOperatorDescriptors.remove(opId)
        invalidSet += opId
      }
      logger.info("Operator {} cache invalidated.", opId)
    }

    def invalidateOperatorCacheRecursivelyForCacheStatusUpdate(opId: String): Unit = {
      invalidateOperatorCacheForCacheStatusUpdate(opId)
      workflowDAG
        .getDownstream(opId)
        .foreach(desc => {
          invalidateOperatorCacheRecursivelyForCacheStatusUpdate(desc.operatorID)
        })
    }

    def isUpdatedForCacheStatusUpdate(opId: String): Boolean = {
      if (!operatorRecord.contains(opId)) {
        operatorRecord += ((opId, getWorkflowVertex(workflowDAG.getOperator(opId))))
        logger.info("Vertex {} is not recorded.", operatorRecord(opId))
        true
      } else if (workflowInfo.cachedOperatorIds.contains(opId)) {
        !operatorRecord(opId).equals(getWorkflowVertex(workflowDAG.getOperator(opId)))
      } else {
        val vertex = getWorkflowVertex(workflowDAG.getOperator(opId))
        if (!operatorRecord(opId).equals(vertex)) {
          operatorRecord(opId) = vertex
          logger.info("Vertex {} is updated.", operatorRecord(opId))
          true
        } else if (cachedOperatorDescriptors.contains(opId)) {
          !workflowInfo.cachedOperatorIds.contains(opId)
        } else {
          logger.info("Operator: {} is not updated.", operatorRecord(opId))
          false
        }
      }
    }

    def invalidateIfUpdatedForCacheStatusUpdate(opId: String): Unit = {
      if (visitedOpIdSet.contains(opId)) {
        return
      }
      visitedOpIdSet += opId
      logger.info(
        "Checking update status of operator {}.",
        workflowDAG.getOperator(opId).toString
      )
      if (isUpdatedForCacheStatusUpdate(opId)) {
        invalidateOperatorCacheForCacheStatusUpdate(opId)
        workflowDAG
          .getDownstream(opId)
          .map(op => op.operatorID)
          .foreach(invalidateOperatorCacheRecursivelyForCacheStatusUpdate)
      }
    }

    val opIter = workflowDAG.jgraphtDag.iterator()
    while (opIter.hasNext) {
      val opId = opIter.next()
      if (!visitedOpIdSet.contains(opId)) {
        invalidateIfUpdatedForCacheStatusUpdate(opId)
      }
    }
    workflowInfo.operators
      .map(op => op.operatorID)
      .filterNot(visitedOpIdSet.contains)
      .foreach(invalidSet.+=)
    invalidSet
  }

  private def checkCacheValidity(): Unit = {
    val opIter = workflowDAG.jgraphtDag.iterator()
    while (opIter.hasNext) {
      val id = opIter.next()
      if (!visitedOpIdSet.contains(id)) {
        invalidateIfUpdated(id)
      }
    }
  }

  private def invalidateIfUpdated(operatorId: String): Unit = {
    if (visitedOpIdSet.contains(operatorId)) {
      return
    }
    visitedOpIdSet += operatorId
    logger.info(
      "Checking update status of operator {}.",
      workflowDAG.getOperator(operatorId).toString
    )
    if (isUpdated(operatorId)) {
      invalidateOperatorCache(operatorId)
      workflowDAG
        .getDownstream(operatorId)
        .map(op => op.operatorID)
        .foreach(invalidateOperatorCacheRecursively)
    }
  }

  def isUpdated(opId: String): Boolean = {
    if (!operatorRecord.contains(opId)) {
      operatorRecord += ((opId, getWorkflowVertex(workflowDAG.getOperator(opId))))
      logger.info("Vertex {} is not recorded.", operatorRecord(opId))
      true
    } else if (workflowInfo.cachedOperatorIds.contains(opId)) {
      if (cachedOperatorDescriptors.contains(opId)) {
        val vertex = getWorkflowVertex(workflowDAG.getOperator(opId))
        if (operatorRecord(opId).equals(vertex)) {
          false
        } else {
          operatorRecord(opId) = vertex
          true
        }
      } else {
        true
      }
    } else {
      val vertex = getWorkflowVertex(workflowDAG.getOperator(opId))
      if (!operatorRecord(opId).equals(vertex)) {
        operatorRecord(opId) = vertex
        logger.info("Vertex {} is updated.", operatorRecord(opId))
        true
      } else if (cachedOperatorDescriptors.contains(opId)) {
        !workflowInfo.cachedOperatorIds.contains(opId)
      } else {
        logger.info("Operator: {} is not updated.", operatorRecord(opId))
        false
      }
    }
  }

  private def invalidateOperatorCache(opId: String): Unit = {
    if (cachedOperatorDescriptors.contains(opId)) {
      cachedOperatorDescriptors.remove(opId)
      opResultStorage.remove(cacheSinkOperatorDescriptors(opId).operatorID)
      cacheSinkOperatorDescriptors.remove(opId)
      cacheSourceOperatorDescriptors.remove(opId)
    }
    logger.info("Operator {} cache invalidated.", opId)
  }

  private def invalidateOperatorCacheRecursively(opId: String): Unit = {
    invalidateOperatorCache(opId)
    workflowDAG
      .getDownstream(opId)
      .foreach(desc => {
        invalidateOperatorCacheRecursively(desc.operatorID)
      })
  }

  private def isCacheEnabled(desc: OperatorDescriptor): Boolean = {
    if (!workflowInfo.cachedOperatorIds.contains(desc.operatorID)) {
      cachedOperatorDescriptors.remove(desc.operatorID)
      logger.info("Operator {} cache not enabled.", desc)
      return false
    }
    logger.info("Operator {} cache enabled.", desc)
    true
  }

  private def isCacheValid(desc: OperatorDescriptor): Boolean = {
    logger.info("Checking the cache validity of operator {}.", desc.toString)
    assert(isCacheEnabled(desc))
    if (cachedOperatorDescriptors.contains(desc.operatorID)) {
      if (
        getCachedOperator(desc).equals(
          desc
        ) && !rewrittenToCacheOperatorIDs.contains(
          desc.operatorID
        )
      ) {
        logger.info("Operator {} cache valid.", desc)
        return true
      }
      logger.info("Operator {} cache invalid.", desc)
    } else {
      logger.info("cachedOperators: {}.", cachedOperatorDescriptors.toString())
      logger.info("Operator {} is never cached.", desc)
    }
    false
  }

  private def getCachedOperator(desc: OperatorDescriptor): OperatorDescriptor = {
    assert(cachedOperatorDescriptors.contains(desc.operatorID))
    cachedOperatorDescriptors(desc.operatorID)
  }

  private def generateCacheSinkOperator(
      operatorDescriptor: OperatorDescriptor
  ): ProgressiveSinkOpDesc = {
    logger.info("Generating CacheSinkOperator for operator {}.", operatorDescriptor.toString)
    cachedOperatorDescriptors += ((operatorDescriptor.operatorID, copyOperator(operatorDescriptor)))
    logger.info(
      "Operator: {} added to cachedOperators: {}.",
      operatorDescriptor.toString,
      cachedOperatorDescriptors.toString()
    )
    val cacheSinkOperator = new ProgressiveSinkOpDesc()
    cacheSinkOperator.setCachedUpstreamId(operatorDescriptor.operatorID)
    cacheSinkOperatorDescriptors += ((operatorDescriptor.operatorID, cacheSinkOperator))
    val cacheSourceOperator = new CacheSourceOpDesc(operatorDescriptor.operatorID, opResultStorage)
    cacheSourceOperatorDescriptors += ((operatorDescriptor.operatorID, cacheSourceOperator))
    cacheSinkOperator
  }

  private def getCacheSourceOperator(
      operatorDescriptor: OperatorDescriptor
  ): CacheSourceOpDesc = {
    val cacheSourceOperator = cacheSourceOperatorDescriptors(operatorDescriptor.operatorID)
    cacheSourceOperator.schema = cacheSinkOperatorDescriptors(
      operatorDescriptor.operatorID
    ).getStorage.getSchema
    cacheSourceOperator
  }

  private def generateCacheSinkLink(
      dest: OperatorDescriptor,
      src: OperatorDescriptor
  ): OperatorLink = {
    val destPort: OperatorPort = OperatorPort(dest.operatorID, 0)
    val srcPort: OperatorPort = OperatorPort(src.operatorID, 0)
    OperatorLink(srcPort, destPort)
  }

  def getWorkflowVertex(desc: OperatorDescriptor): WorkflowVertex = {
    val opInVertex = copyOperator(desc)
    val links = mutable.HashSet[OperatorLink]()
    if (!workflowDAG.operators.contains(desc.operatorID)) {
      null
    } else {
      workflowDAG.jgraphtDag.incomingEdgesOf(opInVertex.operatorID).forEach(link => links.+=(link))
      WorkflowVertex(opInVertex, links)
    }
  }

}
