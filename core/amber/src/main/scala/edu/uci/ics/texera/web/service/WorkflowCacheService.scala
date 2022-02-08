package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.web.model.websocket.event.{CacheStatusUpdateEvent, TexeraWebSocketEvent}
import edu.uci.ics.texera.web.{SubscriptionManager, WebsocketInput}
import edu.uci.ics.texera.web.model.websocket.request.CacheStatusUpdateRequest
import edu.uci.ics.texera.web.storage.WorkflowStateStore
import edu.uci.ics.texera.web.workflowcachestate.CacheState.{INVALID, VALID}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.{WorkflowInfo, WorkflowRewriter, WorkflowVertex}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc

import scala.collection.mutable

object WorkflowCacheService extends LazyLogging {
  def isAvailable: Boolean = AmberUtils.amberConfig.getBoolean("cache.enabled")
}

class WorkflowCacheService(
    opResultStorage: OpResultStorage,
    stateStore: WorkflowStateStore,
    wsInput: WebsocketInput
) extends SubscriptionManager
    with LazyLogging {

  val cachedOperators: mutable.HashMap[String, OperatorDescriptor] =
    mutable.HashMap[String, OperatorDescriptor]()
  val cacheSourceOperators: mutable.HashMap[String, CacheSourceOpDesc] =
    mutable.HashMap[String, CacheSourceOpDesc]()
  val cacheSinkOperators: mutable.HashMap[String, ProgressiveSinkOpDesc] =
    mutable.HashMap[String, ProgressiveSinkOpDesc]()
  val operatorRecord: mutable.HashMap[String, WorkflowVertex] =
    mutable.HashMap[String, WorkflowVertex]()

  addSubscription(
    stateStore.cacheStore.registerDiffHandler((oldState, newState) => {
      Iterable(CacheStatusUpdateEvent(newState.operatorInfo.map {
        case (k, v) => (k, if (v.isInvalid) "cache invalid" else "cache valid")
      }))
    })
  )

  addSubscription(wsInput.subscribe((req: CacheStatusUpdateRequest, uidOpt) => {
    updateCacheStatus(req)
  }))

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
    stateStore.cacheStore.updateState { oldState =>
      oldState.withOperatorInfo(
        request.cachedOperatorIds
          .filter(cachedOperators.contains)
          .map(id => {
            if (cachedOperators.contains(id)) {
              if (!invalidSet.contains(id)) {
                (id, VALID)
              } else {
                (id, INVALID)
              }
            } else {
              (id, INVALID)
            }
          })
          .toMap
      )
    }
  }
}
