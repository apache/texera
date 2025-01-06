package edu.uci.ics.texera.web.service

import akka.actor.Cancellable
import com.fasterxml.jackson.annotation.{JsonTypeInfo, JsonTypeName}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.StorageConfig
import edu.uci.ics.amber.core.storage.result.OpResultStorage.MONGODB
import edu.uci.ics.amber.core.storage.result._
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.workflow.{PhysicalOp, PhysicalPlan}
import edu.uci.ics.amber.engine.architecture.controller.{ExecutionStateUpdate, FatalError}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  FAILED,
  KILLED,
  RUNNING
}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.executionruntimestate.ExecutionMetadataStore
import edu.uci.ics.amber.engine.common.{AmberConfig, AmberRuntime}
import edu.uci.ics.amber.core.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.OutputPort.OutputMode
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.texera.web.SubscriptionManager
import edu.uci.ics.texera.web.model.websocket.event.{
  PaginatedResultEvent,
  TexeraWebSocketEvent,
  WebResultUpdateEvent
}
import edu.uci.ics.texera.web.model.websocket.request.ResultPaginationRequest
import edu.uci.ics.texera.web.storage.{ExecutionStateStore, WorkflowStateStore}

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.duration.DurationInt

object ExecutionResultService {

  private val defaultPageSize: Int = 5

  /**
    * convert Tuple from engine's format to JSON format
    */
  private def tuplesToWebData(
      mode: WebOutputMode,
      table: List[Tuple]
  ): WebDataUpdate = {
    val tableInJson = table.map(t => t.asKeyValuePairJson())
    WebDataUpdate(mode, tableInJson)
  }

  /**
    * For SET_SNAPSHOT output mode: result is the latest snapshot
    * FOR SET_DELTA output mode:
    *   - for insert-only delta: effectively the same as latest snapshot
    *   - for insert-retract delta: the union of all delta outputs, not compacted to a snapshot
    *
    * Produces the WebResultUpdate to send to frontend from a result update from the engine.
    */
  private def convertWebResultUpdate(
      workflowIdentity: WorkflowIdentity,
      physicalOps: List[PhysicalOp],
      oldTupleCount: Int,
      newTupleCount: Int
  ): WebResultUpdate = {
    val outputMode = physicalOps
      .flatMap(op => op.outputPorts)
      .filter({
        case (portId, (port, links, schema)) => !portId.internal
      })
      .map({
        case (portId, (port, links, schema)) => port.mode
      })
      .head

    val webOutputMode: WebOutputMode = {
      outputMode match {
        // currently, only table outputs are using these modes
        case OutputMode.SET_DELTA    => SetDeltaMode()
        case OutputMode.SET_SNAPSHOT => PaginationMode()

        // currently, only visualizations are using single snapshot mode
        case OutputMode.SINGLE_SNAPSHOT => SetSnapshotMode()
      }
    }

    val storage =
      ResultStorage
        .getOpResultStorage(workflowIdentity)
        .get(OpResultStorage.createStorageKey(physicalOps.head.id.logicalOpId, PortIdentity()))
    val webUpdate = webOutputMode match {
      case PaginationMode() =>
        val numTuples = storage.getCount
        val maxPageIndex =
          Math.ceil(numTuples / defaultPageSize.toDouble).toInt
        WebPaginationUpdate(
          PaginationMode(),
          newTupleCount,
          (1 to maxPageIndex).toList
        )
      case SetSnapshotMode() =>
        tuplesToWebData(webOutputMode, storage.get().toList)
      case SetDeltaMode() =>
        val deltaList = storage.getAfter(oldTupleCount).toList
        tuplesToWebData(webOutputMode, deltaList)

      case _ =>
        throw new RuntimeException(
          "update mode combination not supported: " + (webOutputMode, outputMode)
        )
    }
    webUpdate
  }

  /**
    * Behavior for different web output modes:
    *  - PaginationMode   (used by view result operator)
    *     - send new number of tuples and dirty page index
    *  - SetSnapshotMode  (used by visualization in snapshot mode)
    *     - send entire snapshot result to frontend
    *  - SetDeltaMode     (used by visualization in delta mode)
    *     - send incremental delta result to frontend
    */
  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  sealed abstract class WebOutputMode extends Product with Serializable

  /**
    * The result update of one operator that will be sent to the frontend.
    * Can be either WebPaginationUpdate (for PaginationMode)
    * or WebDataUpdate (for SetSnapshotMode or SetDeltaMode)
    */
  sealed abstract class WebResultUpdate extends Product with Serializable

  @JsonTypeName("PaginationMode")
  final case class PaginationMode() extends WebOutputMode

  @JsonTypeName("SetSnapshotMode")
  final case class SetSnapshotMode() extends WebOutputMode

  @JsonTypeName("SetDeltaMode")
  final case class SetDeltaMode() extends WebOutputMode

  case class WebPaginationUpdate(
      mode: PaginationMode,
      totalNumTuples: Long,
      dirtyPageIndices: List[Int]
  ) extends WebResultUpdate

  case class WebDataUpdate(mode: WebOutputMode, table: List[ObjectNode]) extends WebResultUpdate

}

/**
  * ExecutionResultService manages the materialized result of all sink operators in one workflow execution.
  *
  * On each result update from the engine, WorkflowResultService
  *  - update the result data for each operator,
  *  - send result update event to the frontend
  */
class ExecutionResultService(
    workflowIdentity: WorkflowIdentity,
    val workflowStateStore: WorkflowStateStore
) extends SubscriptionManager
    with LazyLogging {
  private val resultPullingFrequency = AmberConfig.executionResultPollingInSecs
  private var resultUpdateCancellable: Cancellable = _

  def attachToExecution(
      stateStore: ExecutionStateStore,
      physicalPlan: PhysicalPlan,
      client: AmberClient
  ): Unit = {

    if (resultUpdateCancellable != null && !resultUpdateCancellable.isCancelled) {
      resultUpdateCancellable.cancel()
    }

    unsubscribeAll()

    addSubscription(stateStore.metadataStore.getStateObservable.subscribe {
      newState: ExecutionMetadataStore =>
        {
          if (newState.state == RUNNING) {
            if (resultUpdateCancellable == null || resultUpdateCancellable.isCancelled) {
              resultUpdateCancellable = AmberRuntime
                .scheduleRecurringCallThroughActorSystem(
                  2.seconds,
                  resultPullingFrequency.seconds
                ) {
                  onResultUpdate(physicalPlan)
                }
            }
          } else {
            if (resultUpdateCancellable != null) resultUpdateCancellable.cancel()
          }
        }
    })

    addSubscription(
      client
        .registerCallback[ExecutionStateUpdate](evt => {
          if (evt.state == COMPLETED || evt.state == FAILED || evt.state == KILLED) {
            logger.info("Workflow execution terminated. Stop update results.")
            if (resultUpdateCancellable.cancel() || resultUpdateCancellable.isCancelled) {
              // immediately perform final update
              onResultUpdate(physicalPlan)
            }
          }
        })
    )

    addSubscription(
      client.registerCallback[FatalError](_ =>
        if (resultUpdateCancellable != null) {
          resultUpdateCancellable.cancel()
        }
      )
    )

    addSubscription(
      workflowStateStore.resultStore.registerDiffHandler((oldState, newState) => {
        val buf = mutable.HashMap[String, ExecutionResultService.WebResultUpdate]()
        val allTableStats = mutable.Map[String, Map[String, Map[String, Any]]]()
        newState.resultInfo
          .filter(info => {
            // only update those operators with changing tuple count.
            !oldState.resultInfo
              .contains(info._1) || oldState.resultInfo(info._1).tupleCount != info._2.tupleCount
          })
          .foreach {
            case (opId, info) =>
              val oldInfo = oldState.resultInfo.getOrElse(opId, OperatorResultMetadata())
              buf(opId.id) = ExecutionResultService.convertWebResultUpdate(
                workflowIdentity,
                physicalPlan.getPhysicalOpsOfLogicalOp(opId),
                oldInfo.tupleCount,
                info.tupleCount
              )
              if (StorageConfig.resultStorageMode == MONGODB) {
                // using the first port for now. TODO: support multiple ports
                val storageKey = OpResultStorage.createStorageKey(opId, PortIdentity())
                val opStorage = ResultStorage
                  .getOpResultStorage(workflowIdentity)
                  .get(storageKey)
                opStorage match {
                  case mongoDocument: MongoDocument[Tuple] =>
                    val tableCatStats = mongoDocument.getCategoricalStats
                    val tableDateStats = mongoDocument.getDateColStats
                    val tableNumericStats = mongoDocument.getNumericColStats

                    if (
                      tableNumericStats.nonEmpty || tableCatStats.nonEmpty || tableDateStats.nonEmpty
                    ) {
                      allTableStats(opId.id) = tableNumericStats ++ tableCatStats ++ tableDateStats
                    }
                  case _ =>
                }
              }
          }
        Iterable(
          WebResultUpdateEvent(
            buf.toMap,
            allTableStats.toMap,
            StorageConfig.resultStorageMode.toLowerCase
          )
        )
      })
    )

    // clear all the result metadata
    workflowStateStore.resultStore.updateState { _ =>
      WorkflowResultStore() // empty result store
    }

  }

  def handleResultPagination(request: ResultPaginationRequest): TexeraWebSocketEvent = {
    // calculate from index (pageIndex starts from 1 instead of 0)
    val from = request.pageSize * (request.pageIndex - 1)

    // using the first port for now. TODO: support multiple ports
    val storageKey =
      OpResultStorage.createStorageKey(OperatorIdentity(request.operatorID), PortIdentity())
    val paginationIterable = {
      ResultStorage
        .getOpResultStorage(workflowIdentity)
        .get(storageKey)
        .getRange(from, from + request.pageSize)
        .to(Iterable)
    }
    val mappedResults = paginationIterable
      .map(tuple => tuple.asKeyValuePairJson())
      .toList
    val attributes = paginationIterable.headOption
      .map(_.getSchema.getAttributes)
      .getOrElse(List.empty)
    PaginatedResultEvent.apply(request, mappedResults, attributes)
  }

  private def onResultUpdate(physicalPlan: PhysicalPlan): Unit = {
    workflowStateStore.resultStore.updateState { _ =>
      val newInfo: Map[OperatorIdentity, OperatorResultMetadata] = {
        ResultStorage
          .getOpResultStorage(workflowIdentity)
          .getAllKeys
          .filter(!_.startsWith("materialized_"))
          .map(storageKey => {
            val count = ResultStorage
              .getOpResultStorage(workflowIdentity)
              .get(storageKey)
              .getCount
              .toInt

            val (opId, storagePortId) = OpResultStorage.decodeStorageKey(storageKey)

            // Retrieve the mode of the specified output port
            val mode = physicalPlan
              .getPhysicalOpsOfLogicalOp(opId)
              .flatMap(_.outputPorts.get(storagePortId))
              .map(_._1.mode)
              .head

            val changeDetector =
              if (mode == OutputMode.SET_SNAPSHOT) {
                UUID.randomUUID.toString
              } else ""
            (opId, OperatorResultMetadata(count, changeDetector))
          })
          .toMap
      }
      WorkflowResultStore(newInfo)
    }
  }

}
