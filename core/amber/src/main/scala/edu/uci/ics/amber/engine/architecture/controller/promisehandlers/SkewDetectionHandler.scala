package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  Workflow
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.SkewDetectionHandler.{
  ControllerInitiateSkewDetection,
  detectionCallCount,
  firstPhaseRequestsFinished,
  getPreviousWorkerLayer,
  getSkewedAndFreeWorkersEligibleForPauseMitigationPhase,
  getSkewedAndFreeWorkersEligibleForSecondPhase,
  getSkewedAndHelperWorkersEligibleForFirstPhase,
  pauseMitigationRequestsFinished,
  previousSkewDetectionCallFinished,
  secondPhaseRequestsFinished,
  skewedAndHelperInFirstPhase,
  skewedAndHelperInPauseMitigationPhase,
  skewedAndHelperInSecondPhase,
  skewedToHelperMappingHistory,
  skewedToStateTransferDone
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{WorkerLayer, WorkerWorkloadInfo}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseSkewMitigationHandler.PauseSkewMitigation
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SendImmutableStateHandler.SendImmutableState
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SharePartitionHandler.SharePartition
import edu.uci.ics.amber.engine.common.{AmberUtils, Constants}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpExecConfig

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

object SkewDetectionHandler {
  var detectionCallCount = 0
  var previousSkewDetectionCallFinished = true
  var firstPhaseRequestsFinished = true
  var secondPhaseRequestsFinished = true
  var pauseMitigationRequestsFinished = true

  // contains skewed and helper mappings. A mapping does not mean that state
  // has been transferred. For that we need to check `skewedToStateTransferDone`.
  var skewedToHelperMappingHistory =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  // contains skewed worker and whether state has been successfully transferred
  var skewedToStateTransferDone =
    new mutable.HashMap[ActorVirtualIdentity, Boolean]()
  // contains pairs which are in first phase of mitigation
  var skewedAndHelperInFirstPhase =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  // contains pairs which are in second phase of mitigation
  var skewedAndHelperInSecondPhase =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  // During mitigation it may happen that the helper receives too much data. If that happens,
  // we pause the mitigation and revert to the original partitioning logic.
  var skewedAndHelperInPauseMitigationPhase =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()

  final case class ControllerInitiateSkewDetection(
      filterByWorkers: List[ActorVirtualIdentity] = List()
  ) extends ControlCommand[Unit]

  /**
    * worker is eligible for first phase if no mitigation has happened till now or
    * it is in second phase right now.
    */
  def isEligibleForSkewedAndFirstPhase(worker: ActorVirtualIdentity): Boolean = {
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
  def isEligibleForHelper(worker: ActorVirtualIdentity): Boolean = {
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
      loads: mutable.HashMap[ActorVirtualIdentity, WorkerWorkloadInfo]
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
      loads: mutable.HashMap[ActorVirtualIdentity, WorkerWorkloadInfo]
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
    */
  def getSkewedAndHelperWorkersEligibleForFirstPhase(
      loads: mutable.HashMap[ActorVirtualIdentity, WorkerWorkloadInfo]
  ): ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)] = {
    val retPairs = new ArrayBuffer[(ActorVirtualIdentity, ActorVirtualIdentity, Boolean)]()
    // Get workers in increasing load
    val sortedWorkers = loads.keys.toList.sortBy(loads(_).dataInputWorkload)

    sortedWorkers.foreach(w =>
      println(s"\t Workload ${w.toString()}: ${loads(w).dataInputWorkload}")
    )

    for (i <- sortedWorkers.size - 1 to 0 by -1) {
      if (isEligibleForSkewedAndFirstPhase(sortedWorkers(i))) {
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
            if (skewedToStateTransferDone(sortedWorkers(i))) {
              // state transfer is already done
              retPairs.append(
                (sortedWorkers(i), skewedToHelperMappingHistory(sortedWorkers(i)), false)
              )
            } else {
              // state transfer has to be done
              retPairs.append(
                (sortedWorkers(i), skewedToHelperMappingHistory(sortedWorkers(i)), true)
              )
            }
          }
        } else if (i > 0) {
          breakable {
            for (j <- 0 to i - 1) {
              if (
                isEligibleForHelper(sortedWorkers(j)) && passSkewTest(
                  sortedWorkers(i),
                  sortedWorkers(j),
                  loads
                )
              ) {
                // this is the first time this skewed and helper workers are undergoing mitigation
                retPairs.append((sortedWorkers(i), sortedWorkers(j), true))
                skewedToHelperMappingHistory(sortedWorkers(i)) = sortedWorkers(j)
                skewedToStateTransferDone(sortedWorkers(i)) = false
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
  def getPreviousWorkerLayer(opId: OperatorIdentity, workflow: Workflow): WorkerLayer = {
    val upstreamOps = workflow.getDirectUpstreamOperators(opId)
    // Below implementation finds the probe input operator for the join operator
    val probeOpId = upstreamOps
      .find(uOpId =>
        workflow.getOperator(uOpId).topology.layers.last.id != workflow
          .getOperator(opId)
          .asInstanceOf[HashJoinOpExecConfig[Any]]
          .buildTable
          .from
      )
      .get

    workflow.getOperator(probeOpId).topology.layers.last
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
      prevWorkerLayer: WorkerLayer,
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
      prevWorkerLayer: WorkerLayer,
      skewedWorker: ActorVirtualIdentity,
      helperWorker: ActorVirtualIdentity
  ): Future[Seq[Boolean]] = {
    val futures = new ArrayBuffer[Future[Boolean]]()
    prevWorkerLayer.workers.keys.foreach(id => {
      if (
        workloadSamples.contains(id) && workloadSamples(id)
          .contains(skewedWorker) && workloadSamples(id)
          .contains(helperWorker)
      ) {
        // Second phase requires that the samples for both skewed and helper workers
        // are recorded at the previous worker `id`. This will be used to partition the
        // incoming data for the skewed worker.
        var skewedLoad = AmberUtils.predictedWorkload(workloadSamples(id)(skewedWorker))
        var helperLoad = AmberUtils.predictedWorkload(workloadSamples(id)(helperWorker))
        var redirectNumerator = ((skewedLoad - helperLoad) / 2).toLong
        workloadSamples(id)(skewedWorker) = new ArrayBuffer[Long]()
        workloadSamples(id)(helperWorker) = new ArrayBuffer[Long]()
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
      prevWorkerLayer: WorkerLayer,
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
      !previousSkewDetectionCallFinished || !firstPhaseRequestsFinished || !secondPhaseRequestsFinished || !pauseMitigationRequestsFinished
    ) {
      Future.Done
    } else {
      previousSkewDetectionCallFinished = false
      detectionCallCount += 1

      workflow.getAllOperators.foreach(opConfig => {
        if (opConfig.isInstanceOf[HashJoinOpExecConfig[Any]]) {
          // Skew handling is only for hash-join operator for now.
          // 1: Find the skewed and helper worker that need first phase.
          val skewedAndHelperPairsForFirstPhase =
            getSkewedAndHelperWorkersEligibleForFirstPhase(opConfig.workerToWorkloadInfo)
          skewedAndHelperPairsForFirstPhase.foreach(skewedAndHelper =>
            logger.info(
              s"Reshape #${detectionCallCount}: First phase process begins - Skewed ${skewedAndHelper._1
                .toString()} :: Helper ${skewedAndHelper._2
                .toString()} - Replication required: ${skewedAndHelper._3.toString()}"
            )
          )

          // 2: Do state transfer if needed and first phase
          firstPhaseRequestsFinished = false
          var firstPhaseFinishedCount = 0
          val prevWorkerLayer = getPreviousWorkerLayer(opConfig.getOperatorIdentity, workflow)
          skewedAndHelperPairsForFirstPhase.foreach(skewedAndHelper => {
            val currSkewedWorker = skewedAndHelper._1
            val currHelperWorker = skewedAndHelper._2
            val stateTransferNeeded = skewedAndHelper._3
            if (stateTransferNeeded) {
              send(SendImmutableState(currHelperWorker), currSkewedWorker).map(
                stateTransferSuccessful => {
                  if (stateTransferSuccessful) {
                    skewedToStateTransferDone(currSkewedWorker) = true
                    logger.info(
                      s"Reshape #${detectionCallCount}: State transfer completed - ${currSkewedWorker
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
                        skewedAndHelperInFirstPhase(currSkewedWorker) = currHelperWorker
                        skewedAndHelperInSecondPhase.remove(currSkewedWorker)
                        skewedAndHelperInPauseMitigationPhase.remove(currSkewedWorker)
                      }
                      logger.info(
                        s"Reshape #${detectionCallCount}: First phase request finished for ${currSkewedWorker
                          .toString()} to ${currHelperWorker.toString()}"
                      )
                      firstPhaseFinishedCount += 1
                      if (firstPhaseFinishedCount == skewedAndHelperPairsForFirstPhase.size) {
                        logger.info(
                          s"Reshape #${detectionCallCount}: First phase requests completed for ${skewedAndHelperPairsForFirstPhase.size} pairs"
                        )
                        firstPhaseRequestsFinished = true
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
                    skewedAndHelperInFirstPhase(currSkewedWorker) = currHelperWorker
                    skewedAndHelperInSecondPhase.remove(currSkewedWorker)
                    skewedAndHelperInPauseMitigationPhase.remove(currSkewedWorker)
                  }
                  logger.info(
                    s"Reshape #${detectionCallCount}: First phase request finished for ${currSkewedWorker
                      .toString()} to ${currHelperWorker.toString()}"
                  )
                  firstPhaseFinishedCount += 1
                  if (firstPhaseFinishedCount == skewedAndHelperPairsForFirstPhase.size) {
                    logger.info(
                      s"Reshape #${detectionCallCount}: First phase requests completed for ${skewedAndHelperPairsForFirstPhase.size} pairs"
                    )
                    firstPhaseRequestsFinished = true
                  }
                })
              )
            }
          })

          // 3: Start second phase for pairs where helpers that have caught up with skewed
          secondPhaseRequestsFinished = false
          val skewedAndHelperPairsForSecondPhase =
            getSkewedAndFreeWorkersEligibleForSecondPhase(opConfig.workerToWorkloadInfo)
          skewedAndHelperPairsForSecondPhase.foreach(skewedAndHelper =>
            logger.info(
              s"Reshape #${detectionCallCount}: Second phase request begins - Skewed ${skewedAndHelper._1
                .toString()} :: Helper ${skewedAndHelper._2
                .toString()}"
            )
          )
          val allPairsSecondPhaseFutures = new ArrayBuffer[Future[Seq[Boolean]]]()
          skewedAndHelperPairsForSecondPhase.foreach(sh => {
            val currSkewedWorker = sh._1
            val currHelperWorker = sh._2
            allPairsSecondPhaseFutures.append(
              implementSecondPhasePartitioning(prevWorkerLayer, currSkewedWorker, currHelperWorker)
                .onSuccess(resultsFromPrevWorker => {
                  if (!resultsFromPrevWorker.contains(false)) {
                    skewedAndHelperInSecondPhase(currSkewedWorker) = currHelperWorker
                    skewedAndHelperInFirstPhase.remove(currSkewedWorker)
                    skewedAndHelperInPauseMitigationPhase.remove(currSkewedWorker)
                  }
                })
            )
          })
          Future
            .collect(allPairsSecondPhaseFutures)
            .onSuccess(_ => {
              secondPhaseRequestsFinished = true
              logger.info(
                s"Reshape #${detectionCallCount}: Second phase requests completed for ${skewedAndHelperPairsForSecondPhase.size} pairs"
              )
            })

          // 4: Pause mitigation for pairs where helpers have become overloaded
          pauseMitigationRequestsFinished = false
          val skewedAndHelperPairsForPauseMitigationPhase =
            getSkewedAndFreeWorkersEligibleForPauseMitigationPhase(opConfig.workerToWorkloadInfo)
          skewedAndHelperPairsForPauseMitigationPhase.foreach(skewedAndHelper =>
            logger.info(
              s"Reshape #${detectionCallCount}: Pause Mitigation phase request begins - Skewed ${skewedAndHelper._1
                .toString()} :: Helper ${skewedAndHelper._2
                .toString()}"
            )
          )
          val allPairsPauseMitigationFutures = new ArrayBuffer[Future[Seq[Boolean]]]()
          skewedAndHelperPairsForPauseMitigationPhase.foreach(sh => {
            val currSkewedWorker = sh._1
            val currHelperWorker = sh._2
            allPairsPauseMitigationFutures.append(
              implementPauseMitigation(prevWorkerLayer, currSkewedWorker, currHelperWorker)
                .onSuccess(resultsFromPrevWorker => {
                  if (!resultsFromPrevWorker.contains(false)) {
                    skewedAndHelperInPauseMitigationPhase(currSkewedWorker) = currHelperWorker
                    skewedAndHelperInFirstPhase.remove(currSkewedWorker)
                    skewedAndHelperInSecondPhase.remove(currSkewedWorker)
                  }
                })
            )
          })
          Future
            .collect(allPairsPauseMitigationFutures)
            .onSuccess(_ => {
              pauseMitigationRequestsFinished = true
              logger.info(
                s"Reshape #${detectionCallCount}: Pause Mitigation phase requests completed for ${skewedAndHelperPairsForPauseMitigationPhase.size} pairs"
              )
            })

        }
      })
      previousSkewDetectionCallFinished = true
      detectionCallCount += 1
      Future.Done
    }
  })
}
