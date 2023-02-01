package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.SkewDetectionHandler._
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  Workflow
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerWorkloadInfo
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseSkewMitigationHandler.PauseSkewMitigation
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SendImmutableStateOrNotifyHelperHandler.SendImmutableStateOrNotifyHelper
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SharePartitionHandler.SharePartition
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExec
import edu.uci.ics.texera.workflow.operators.sortPartitions.SortPartitionOpExec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object SkewDetectionHandler {

  final case class ControllerInitiateSkewDetection(
      filterByWorkers: List[ActorVirtualIdentity] = List()
  ) extends ControlCommand[Unit]

  /**
    * worker is eligible for first phase if no mitigation has happened till now or
    * it is in second phase right now.
    */
  def isEligibleForSkewedAndFirstPhase(
      worker: ActorVirtualIdentity,
      skewedToHelperMappingHistory: mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity],
      skewedAndHelperInFirstPhase: mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]
  ): Boolean = {
    !skewedToHelperMappingHistory.values.toList.contains(
      worker
    ) &&
    !skewedAndHelperInFirstPhase.keySet.contains(
      worker
    )
  }

  /**
    * worker is eligible for being a helper if it is being used in neither of the phases.
    */
  def isEligibleForHelper(
      worker: ActorVirtualIdentity,
      skewedToHelperMappingHistory: mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]
  ): Boolean = {
    !skewedToHelperMappingHistory.keySet.contains(
      worker
    ) && !skewedToHelperMappingHistory.values.toList.contains(
      worker
    )
  }

  def passSkewTest(
      skewedWorkerCand: ActorVirtualIdentity,
      helperWorkerCand: ActorVirtualIdentity,
      loads: mutable.HashMap[ActorVirtualIdentity, WorkerWorkloadInfo]
  ): Boolean = {
    if (
      loads(
        skewedWorkerCand
      ).dataInputWorkload / Constants.defaultBatchSize > Constants.reshapeEtaThreshold && (loads(
        skewedWorkerCand
      ).dataInputWorkload / Constants.defaultBatchSize > Constants.reshapeTauThreshold + loads(
        helperWorkerCand
      ).dataInputWorkload / Constants.defaultBatchSize)
    ) {
      return true
    }
    false
  }

  /**
    * During mitigation it may happen that the helper receives too much data. If that happens,
    * we pause the mitigation and revert to the original partitioning logic. This function
    * uses the workload metrics to evaluate if a helper is getting overloaded.
    *
    * @return array of skewed and helper workers where the helper is getting overloaded
    */
  def getSkewedAndFreeWorkersEligibleForPauseMitigationPhase(
      loads: mutable.HashMap[ActorVirtualIdentity, WorkerWorkloadInfo],
      skewedAndHelperInFirstPhase: mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity],
      skewedAndHelperInSecondPhase: mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity],
      skewedAndHelperInPauseMitigationPhase: mutable.HashMap[
        ActorVirtualIdentity,
        ActorVirtualIdentity
      ]
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)] = {
    val retPairs = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)]()
    // Get workers in increasing load
    val sortedWorkers = loads.keys.toList.sortBy(loads(_).dataInputWorkload)
    val freeWorkersInFirstPhase = skewedAndHelperInFirstPhase.values.toList
    val freeWorkersInSecondPhase = skewedAndHelperInSecondPhase.values.toList
    val freeWorkersInPauseMitigationPhase = skewedAndHelperInPauseMitigationPhase.values.toList
    for (i <- 0 to sortedWorkers.size - 1) {
      if (
        !freeWorkersInPauseMitigationPhase.contains(sortedWorkers(i)) && (freeWorkersInFirstPhase
          .contains(sortedWorkers(i)) || freeWorkersInSecondPhase.contains(sortedWorkers(i)))
      ) {
        // the free worker is in first or second phase, but is not in the pause-mitigation phase
        var skewedCounterpart: ActorVirtualIdentity = null
        skewedAndHelperInFirstPhase.keys.foreach(sw => {
          if (skewedAndHelperInFirstPhase(sw) == sortedWorkers(i)) {
            skewedCounterpart = sw
          }
        })
        if (skewedCounterpart == null) {
          skewedAndHelperInSecondPhase.keys.foreach(sw => {
            if (skewedAndHelperInSecondPhase(sw) == sortedWorkers(i)) {
              skewedCounterpart = sw
            }
          })
        }

        if (
          skewedCounterpart != null &&
          passSkewTest(
            sortedWorkers(i),
            skewedCounterpart,
            loads
          )
        ) {
          retPairs.append((skewedCounterpart, sortedWorkers(i)))
        }
      }
    }
    retPairs
  }

  /**
    * returns an array of (skewedWorker, helperWorker) that are ready to go into second phase
    */
  def getSkewedAndFreeWorkersEligibleForSecondPhase(
      loads: mutable.HashMap[ActorVirtualIdentity, WorkerWorkloadInfo],
      skewedAndHelperInFirstPhase: mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)] = {
    val retPairs = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity)]()
    skewedAndHelperInFirstPhase.keys.foreach(skewedWorker => {
      if (
        loads(skewedWorker).dataInputWorkload <= loads(
          skewedAndHelperInFirstPhase(skewedWorker)
        ).dataInputWorkload && (loads(
          skewedAndHelperInFirstPhase(skewedWorker)
        ).dataInputWorkload - loads(
          skewedWorker
        ).dataInputWorkload < Constants.reshapeHelperOverloadThreshold)
      ) {
        // The skewed worker load has become less than helper worker but the helper worker has not become too overloaded
        retPairs.append((skewedWorker, skewedAndHelperInFirstPhase(skewedWorker)))
      }
    })
    retPairs
  }

  /** *
    * returns an array of (skewedWorker, freeWorker, whether state replication needs to be done)
    *
    * For immutable state operators like hash join, there is an actual state replication as shown
    * by the third argument. For mutable state operators like sort, the third argument just signifies
    * whether the skewed worker has been told that it will sharing state with helper.
    */
  def getSkewedAndHelperWorkersEligibleForFirstPhase(
      loads: mutable.HashMap[ActorVirtualIdentity, WorkerWorkloadInfo],
      skewedToHelperMappingHistory: mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity],
      skewedToStateTransferOrIntimationDone: mutable.HashMap[ActorVirtualIdentity, Boolean],
      skewedAndHelperInFirstPhase: mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)] = {
    val retPairs = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)]()
    // Get workers in increasing load
    val sortedWorkers = loads.keys.toList.sortBy(loads(_).dataInputWorkload)

    for (i <- sortedWorkers.size - 1 to 0 by -1) {
      if (
        isEligibleForSkewedAndFirstPhase(
          sortedWorkers(i),
          skewedToHelperMappingHistory,
          skewedAndHelperInFirstPhase
        )
      ) {
        if (skewedToHelperMappingHistory.keySet.contains(sortedWorkers(i))) {
          // worker has been previously paired with some helper.
          // So, that helper will be used again.
          if (
            passSkewTest(
              sortedWorkers(i),
              skewedToHelperMappingHistory(sortedWorkers(i)),
              loads
            )
          ) {
            retPairs.append(
              (
                sortedWorkers(i),
                skewedToHelperMappingHistory(sortedWorkers(i)),
                !skewedToStateTransferOrIntimationDone(sortedWorkers(i))
              )
            )
          }
        } else if (i > 0) {
          breakable {
            for (j <- 0 to i - 1) {
              if (
                isEligibleForHelper(sortedWorkers(j), skewedToHelperMappingHistory) && passSkewTest(
                  sortedWorkers(i),
                  sortedWorkers(j),
                  loads
                )
              ) {
                // this is the first time this skewed and helper workers are undergoing mitigation
                retPairs.append((sortedWorkers(i), sortedWorkers(j), true))
                skewedToHelperMappingHistory(sortedWorkers(i)) = sortedWorkers(j)
                skewedToStateTransferOrIntimationDone(sortedWorkers(i)) = false
                break
              }
            }
          }
        }
      }
    }

    retPairs
  }

  /**
    * Get the worker layer from the previous operator where the partitioning logic will be changed
    * by Reshape.
    */
  def getPreviousWorkerLayer(opId: LayerIdentity, workflow: Workflow): OpExecConfig = {
    val upstreamLayers = workflow.getUpStreamConnectedWorkerLayers(opId).values.toList

    if (workflow.getOperator(opId).opExecClass == classOf[HashJoinOpExec[_]]) {
      upstreamLayers
        .find(layer => {
          val buildTableLinkId = layer.inputToOrdinalMapping.find(input => input._2 == 0).get._1
          layer.id != buildTableLinkId.from
        })
        .get
    } else {
      // Should be sort operator
      upstreamLayers(0)
    }
  }

  /**
    * Used by Reshape for predicting load on a worker. This is
    * the estimated load using the mean model.
    */
  def predictedWorkload(workloads: ArrayBuffer[Long]): Double = {
    var mean: Double = 0
    workloads.foreach(load => {
      mean = mean + load
    })
    mean = mean / workloads.size
    mean
  }

}

trait SkewDetectionHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  /**
    * Sends `SharePartition` control message to each worker in `prevWorkerLayer` to start the first phase.
    * The message means that data of the skewed worker partition will be shared with the free worker in
    * `skewedAndHelperWorkersList`.
    */
  private def implementFirstPhasePartitioning[T](
      prevWorkerLayer: OpExecConfig,
      skewedWorker: ActorVirtualIdentity,
      helperWorker: ActorVirtualIdentity
  ): Future[Seq[Boolean]] = {

    val futures = new ArrayBuffer[Future[Boolean]]()
    prevWorkerLayer.workers.keys.foreach(id => {
      futures.append(
        send(
          SharePartition(
            skewedWorker,
            helperWorker,
            Constants.reshapeFirstPhaseSharingNumerator,
            Constants.reshapeFirstPhaseSharingDenominator
          ),
          id
        )
      )
    })

    Future.collect(futures)
  }

  private def implementSecondPhasePartitioning[T](
      prevWorkerLayer: OpExecConfig,
      skewedWorker: ActorVirtualIdentity,
      helperWorker: ActorVirtualIdentity
  ): Future[Seq[Boolean]] = {
    val futures = new ArrayBuffer[Future[Boolean]]()
    prevWorkerLayer.workers.keys.foreach(id => {
      if (
        workflowReshapeState.workloadSamples.contains(id) && workflowReshapeState
          .workloadSamples(id)
          .contains(skewedWorker) && workflowReshapeState
          .workloadSamples(id)
          .contains(helperWorker)
      ) {
        // Second phase requires that the samples for both skewed and helper workers
        // are recorded at the previous worker `id`. This will be used to partition the
        // incoming data for the skewed worker.
        var skewedLoad = predictedWorkload(workflowReshapeState.workloadSamples(id)(skewedWorker))
        var helperLoad = predictedWorkload(workflowReshapeState.workloadSamples(id)(helperWorker))
        var redirectNumerator = ((skewedLoad - helperLoad) / 2).toLong
        workflowReshapeState.workloadSamples(id)(skewedWorker) = new ArrayBuffer[Long]()
        workflowReshapeState.workloadSamples(id)(helperWorker) = new ArrayBuffer[Long]()
        if (skewedLoad == 0 || helperLoad > skewedLoad) {
          helperLoad = 0
          skewedLoad = 1
          redirectNumerator = 0
        }
        futures.append(
          send(
            SharePartition(
              skewedWorker,
              helperWorker,
              redirectNumerator,
              skewedLoad.toLong
            ),
            id
          )
        )

      }
    })

    Future.collect(futures)
  }

  private def implementPauseMitigation[T](
      prevWorkerLayer: OpExecConfig,
      skewedWorker: ActorVirtualIdentity,
      helperWorker: ActorVirtualIdentity
  ): Future[Seq[Boolean]] = {
    val futuresArr = new ArrayBuffer[Future[Boolean]]()
    prevWorkerLayer.workers.keys.foreach(id => {
      futuresArr.append(send(PauseSkewMitigation(skewedWorker, helperWorker), id))
    })
    Future.collect(futuresArr)
  }

  registerHandler((msg: ControllerInitiateSkewDetection, sender) => {
    if (
      !workflowReshapeState.previousSkewDetectionCallFinished || !workflowReshapeState.firstPhaseRequestsFinished || !workflowReshapeState.secondPhaseRequestsFinished || !workflowReshapeState.pauseMitigationRequestsFinished
    ) {
      Future.Done
    } else {
      workflowReshapeState.previousSkewDetectionCallFinished = false
      workflowReshapeState.detectionCallCount += 1

      workflow.getAllOperators.foreach(opConfig => {
        if (
          opConfig.opExecClass == classOf[HashJoinOpExec[_]] ||
          opConfig.opExecClass == classOf[SortPartitionOpExec]
        ) {
          // Skew handling is only for hash-join operator for now.
          // 1: Find the skewed and helper worker that need first phase.
          val skewedAndHelperPairsForFirstPhase =
            getSkewedAndHelperWorkersEligibleForFirstPhase(
              opConfig.workerToWorkloadInfo,
              workflowReshapeState.skewedToHelperMappingHistory,
              workflowReshapeState.skewedToStateTransferOrIntimationDone,
              workflowReshapeState.skewedAndHelperInFirstPhase
            )
          skewedAndHelperPairsForFirstPhase.foreach(skewedHelperAndReplication =>
            logger.info(
              s"Reshape ${workflow.getWorkflowId().id} #${workflowReshapeState.detectionCallCount}: First phase process begins - Skewed ${skewedHelperAndReplication._1
                .toString()} :: Helper ${skewedHelperAndReplication._2
                .toString()} - Replication/Intimation required: ${skewedHelperAndReplication._3.toString()}"
            )
          )

          // 2: Do state transfer if needed and first phase
          workflowReshapeState.firstPhaseRequestsFinished = false
          var firstPhaseFinishedCount = 0
          val prevWorkerLayer = getPreviousWorkerLayer(opConfig.id, workflow)
          if (skewedAndHelperPairsForFirstPhase.isEmpty) {
            workflowReshapeState.firstPhaseRequestsFinished = true
          }
          skewedAndHelperPairsForFirstPhase.foreach(skewedAndHelper => {
            val currSkewedWorker = skewedAndHelper._1
            val currHelperWorker = skewedAndHelper._2
            val stateTransferOrIntimationNeeded = skewedAndHelper._3
            if (stateTransferOrIntimationNeeded) {
              send(SendImmutableStateOrNotifyHelper(currHelperWorker), currSkewedWorker).map(
                stateTransferOrIntimationSuccessful => {
                  if (stateTransferOrIntimationSuccessful) {
                    workflowReshapeState.skewedToStateTransferOrIntimationDone(currSkewedWorker) =
                      true
                    logger.info(
                      s"Reshape ${workflow.getWorkflowId().id} #${workflowReshapeState.detectionCallCount}: State transfer/intimation completed - ${currSkewedWorker
                        .toString()} to ${currHelperWorker.toString()}"
                    )
                    implementFirstPhasePartitioning(
                      prevWorkerLayer,
                      currSkewedWorker,
                      currHelperWorker
                    ).onSuccess(resultsFromPrevWorker => {
                      if (resultsFromPrevWorker.contains(true)) {
                        // Even if one of the previous workers did the partitioning change,
                        // the first phase has started
                        workflowReshapeState.skewedAndHelperInFirstPhase(currSkewedWorker) =
                          currHelperWorker
                        workflowReshapeState.skewedAndHelperInSecondPhase.remove(currSkewedWorker)
                        workflowReshapeState.skewedAndHelperInPauseMitigationPhase.remove(
                          currSkewedWorker
                        )
                      }
                      logger.info(
                        s"Reshape ${workflow.getWorkflowId().id} #${workflowReshapeState.detectionCallCount}: First phase request finished for ${currSkewedWorker
                          .toString()} to ${currHelperWorker.toString()}"
                      )
                      firstPhaseFinishedCount += 1
                      if (firstPhaseFinishedCount == skewedAndHelperPairsForFirstPhase.size) {
                        logger.info(
                          s"Reshape ${workflow.getWorkflowId().id} #${workflowReshapeState.detectionCallCount}: First phase requests completed for ${skewedAndHelperPairsForFirstPhase.size} pairs"
                        )
                        workflowReshapeState.firstPhaseRequestsFinished = true
                      }
                    })
                  } else {
                    Future(Array(true))
                  }
                }
              )
            } else {
              Future(true).map(_ =>
                implementFirstPhasePartitioning(
                  prevWorkerLayer,
                  currSkewedWorker,
                  currHelperWorker
                ).onSuccess(resultsFromPrevWorker => {
                  if (resultsFromPrevWorker.contains(true)) {
                    // Even if one of the previous workers did the partitioning change,
                    // the first phase has started
                    workflowReshapeState.skewedAndHelperInFirstPhase(currSkewedWorker) =
                      currHelperWorker
                    workflowReshapeState.skewedAndHelperInSecondPhase.remove(currSkewedWorker)
                    workflowReshapeState.skewedAndHelperInPauseMitigationPhase.remove(
                      currSkewedWorker
                    )
                  }
                  logger.info(
                    s"Reshape ${workflow.getWorkflowId().id} #${workflowReshapeState.detectionCallCount}: First phase request finished for ${currSkewedWorker
                      .toString()} to ${currHelperWorker.toString()}"
                  )
                  firstPhaseFinishedCount += 1
                  if (firstPhaseFinishedCount == skewedAndHelperPairsForFirstPhase.size) {
                    logger.info(
                      s"Reshape ${workflow.getWorkflowId().id} #${workflowReshapeState.detectionCallCount}: First phase requests completed for ${skewedAndHelperPairsForFirstPhase.size} pairs"
                    )
                    workflowReshapeState.firstPhaseRequestsFinished = true
                  }
                })
              )
            }
          })

          // 3: Start second phase for pairs where helpers that have caught up with skewed
          workflowReshapeState.secondPhaseRequestsFinished = false
          val skewedAndHelperPairsForSecondPhase =
            getSkewedAndFreeWorkersEligibleForSecondPhase(
              opConfig.workerToWorkloadInfo,
              workflowReshapeState.skewedAndHelperInFirstPhase
            )
          skewedAndHelperPairsForSecondPhase.foreach(skewedAndHelper =>
            logger.info(
              s"Reshape ${workflow.getWorkflowId().id} #${workflowReshapeState.detectionCallCount}: Second phase request begins - Skewed ${skewedAndHelper._1
                .toString()} :: Helper ${skewedAndHelper._2
                .toString()}"
            )
          )
          if (skewedAndHelperPairsForSecondPhase.isEmpty) {
            workflowReshapeState.secondPhaseRequestsFinished = true
          }
          val allPairsSecondPhaseFutures = new ArrayBuffer[Future[Seq[Boolean]]]()
          skewedAndHelperPairsForSecondPhase.foreach(sh => {
            val currSkewedWorker = sh._1
            val currHelperWorker = sh._2
            allPairsSecondPhaseFutures.append(
              implementSecondPhasePartitioning(prevWorkerLayer, currSkewedWorker, currHelperWorker)
                .onSuccess(resultsFromPrevWorker => {
                  if (!resultsFromPrevWorker.contains(false)) {
                    workflowReshapeState.skewedAndHelperInSecondPhase(currSkewedWorker) =
                      currHelperWorker
                    workflowReshapeState.skewedAndHelperInFirstPhase.remove(currSkewedWorker)
                    workflowReshapeState.skewedAndHelperInPauseMitigationPhase.remove(
                      currSkewedWorker
                    )
                  }
                })
            )
          })
          Future
            .collect(allPairsSecondPhaseFutures)
            .onSuccess(_ => {
              workflowReshapeState.secondPhaseRequestsFinished = true
              logger.info(
                s"Reshape ${workflow.getWorkflowId().id} #${workflowReshapeState.detectionCallCount}: Second phase requests completed for ${skewedAndHelperPairsForSecondPhase.size} pairs"
              )
            })

          // 4: Pause mitigation for pairs where helpers have become overloaded
          workflowReshapeState.pauseMitigationRequestsFinished = false
          val skewedAndHelperPairsForPauseMitigationPhase =
            getSkewedAndFreeWorkersEligibleForPauseMitigationPhase(
              opConfig.workerToWorkloadInfo,
              workflowReshapeState.skewedAndHelperInFirstPhase,
              workflowReshapeState.skewedAndHelperInSecondPhase,
              workflowReshapeState.skewedAndHelperInPauseMitigationPhase
            )
          skewedAndHelperPairsForPauseMitigationPhase.foreach(skewedAndHelper =>
            logger.info(
              s"Reshape ${workflow.getWorkflowId().id} #${workflowReshapeState.detectionCallCount}: Pause Mitigation phase request begins - Skewed ${skewedAndHelper._1
                .toString()} :: Helper ${skewedAndHelper._2
                .toString()}"
            )
          )
          if (skewedAndHelperPairsForPauseMitigationPhase.isEmpty) {
            workflowReshapeState.pauseMitigationRequestsFinished = true
          }
          val allPairsPauseMitigationFutures = new ArrayBuffer[Future[Seq[Boolean]]]()
          skewedAndHelperPairsForPauseMitigationPhase.foreach(sh => {
            val currSkewedWorker = sh._1
            val currHelperWorker = sh._2
            allPairsPauseMitigationFutures.append(
              implementPauseMitigation(prevWorkerLayer, currSkewedWorker, currHelperWorker)
                .onSuccess(resultsFromPrevWorker => {
                  if (!resultsFromPrevWorker.contains(false)) {
                    workflowReshapeState.skewedAndHelperInPauseMitigationPhase(currSkewedWorker) =
                      currHelperWorker
                    workflowReshapeState.skewedAndHelperInFirstPhase.remove(currSkewedWorker)
                    workflowReshapeState.skewedAndHelperInSecondPhase.remove(currSkewedWorker)
                  }
                })
            )
          })
          Future
            .collect(allPairsPauseMitigationFutures)
            .onSuccess(_ => {
              workflowReshapeState.pauseMitigationRequestsFinished = true
              logger.info(
                s"Reshape ${workflow.getWorkflowId().id} #${workflowReshapeState.detectionCallCount}: Pause Mitigation phase requests completed for ${skewedAndHelperPairsForPauseMitigationPhase.size} pairs"
              )
            })

        }
      })
      workflowReshapeState.previousSkewDetectionCallFinished = true
      Future.Done
    }
  })
}
