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
import edu.uci.ics.texera.workflow.common.storage.memory.{JCSOpResultStorage, MemoryOpResultStorage}
import edu.uci.ics.texera.workflow.common.storage.mongo.MongoOpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{WorkflowInfo, WorkflowRewriter, WorkflowVertex}
import edu.uci.ics.texera.workflow.operators.sink.CacheSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import rx.lang.scala.Observer

import scala.collection.mutable

object WorkflowCacheService extends LazyLogging {
  val opResultStorageConfig: Config = ConfigFactory.load("application")
  val storageType: String = AmberUtils.amberConfig.getString("cache.storage").toLowerCase
  def isAvailable: Boolean = storageType != "off"
  var opResultStorage: OpResultStorage = storageType match {
    case "off" =>
      null
    case "memory" =>
      new MemoryOpResultStorage()
    case "jcs" =>
      new JCSOpResultStorage()
    case "mongodb" =>
      new MongoOpResultStorage()
    case _ =>
      throw new RuntimeException(s"invalid storage config $storageType")
  }
  if (isAvailable) {
    logger.info(s"Use $storageType for materialization")
  }
}

class WorkflowCacheService extends SnapshotMulticast[TexeraWebSocketEvent] with LazyLogging {

  val cachedOperators: mutable.HashMap[String, OperatorDescriptor] =
    mutable.HashMap[String, OperatorDescriptor]()
  val cacheSourceOperators: mutable.HashMap[String, CacheSourceOpDesc] =
    mutable.HashMap[String, CacheSourceOpDesc]()
  val cacheSinkOperators: mutable.HashMap[String, CacheSinkOpDesc] =
    mutable.HashMap[String, CacheSinkOpDesc]()
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
      WorkflowCacheService.opResultStorage
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
