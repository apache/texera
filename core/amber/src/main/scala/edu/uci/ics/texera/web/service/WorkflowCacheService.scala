package edu.uci.ics.texera.web.service

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.web.SnapshotMulticast
import edu.uci.ics.texera.web.model.common.CacheStatus
import edu.uci.ics.texera.web.model.websocket.event.{CacheStatusUpdateEvent, TexeraWebSocketEvent}
import edu.uci.ics.texera.web.model.websocket.request.CacheStatusUpdateRequest
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{WorkflowInfo, WorkflowRewriter, WorkflowVertex}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import rx.lang.scala.Observer

import scala.collection.mutable

object WorkflowCacheService extends LazyLogging {
  def isAvailable: Boolean = AmberUtils.amberConfig.getBoolean("cache.enabled")
}

class WorkflowCacheService(opResultStorage: OpResultStorage)
    extends SnapshotMulticast[TexeraWebSocketEvent]
    with LazyLogging {

  val cachedOperators: mutable.HashMap[String, OperatorDescriptor] =
    mutable.HashMap[String, OperatorDescriptor]()
  val cacheSourceOperators: mutable.HashMap[String, CacheSourceOpDesc] =
    mutable.HashMap[String, CacheSourceOpDesc]()
  val cacheSinkOperators: mutable.HashMap[String, ProgressiveSinkOpDesc] =
    mutable.HashMap[String, ProgressiveSinkOpDesc]()
  val operatorRecord: mutable.HashMap[String, WorkflowVertex] =
    mutable.HashMap[String, WorkflowVertex]()
  var cacheStatusMap: Map[String, CacheStatus] = _

  def updateCacheStatus(request: CacheStatusUpdateRequest): Unit = {
    val workflowInfo = WorkflowInfo(request.operators, request.links, request.breakpoints)
    workflowInfo.cachedOperatorIds = request.cachedOperatorIds
    logger.debug(s"Cached operators: $cachedOperators with ${request.cachedOperatorIds}")
    val workflowRewriter = new WorkflowRewriter(
      workflowInfo,
      cachedOperators.clone(),
      cacheSourceOperators.clone(),
      cacheSinkOperators.clone(),
      operatorRecord.clone(),
      opResultStorage
    )

    val invalidSet = workflowRewriter.cacheStatusUpdate()
    cacheStatusMap = request.cachedOperatorIds
      .filter(cachedOperators.contains)
      .map(id => {
        if (cachedOperators.contains(id)) {
          if (!invalidSet.contains(id)) {
            (id, CacheStatus.CACHE_VALID)
          } else {
            (id, CacheStatus.CACHE_INVALID)
          }
        } else {
          (id, CacheStatus.CACHE_INVALID)
        }
      })
      .toMap
    send(CacheStatusUpdateEvent(cacheStatusMap))
  }

  override def sendSnapshotTo(observer: Observer[TexeraWebSocketEvent]): Unit = {
    if (cacheStatusMap != null) {
      observer.onNext(CacheStatusUpdateEvent(cacheStatusMap))
    }
  }
}
