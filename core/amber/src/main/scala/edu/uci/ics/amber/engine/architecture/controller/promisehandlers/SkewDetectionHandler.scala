package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  Workflow
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.SkewDetectionHandler.{
  ControllerInitiateSkewDetection,
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
  skewedToHelperMappingHistory
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
  var previousSkewDetectionCallFinished = true
  var firstPhaseRequestsFinished = true
  var secondPhaseRequestsFinished = true
  var pauseMitigationRequestsFinished = true

  var skewedToHelperMappingHistory =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]() // contains skewed and helper
  var skewedAndHelperInFirstPhase =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
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
    * worker is eligible for free if it is being used in none of the phases.
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
      loads: mutable.HashMap[ActorVirtualIdentity, WorkerWorkloadInfo],
      etaThreshold: Int,
      tauThreshold: Int
  ): Boolean = {
    if (
      loads(
        skewedWorkerCand
      ).dataInputWorkload / Constants.defaultBatchSize > etaThreshold && (loads(
        skewedWorkerCand
      ).dataInputWorkload / Constants.defaultBatchSize > tauThreshold + loads(
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
            loads,
            Constants.reshapeEtaThreshold,
            Constants.reshapeHelperOverloadThreshold
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

    for (i <- sortedWorkers.size - 1 to 0 by -1) {
      if (isEligibleForSkewedAndFirstPhase(sortedWorkers(i))) {
        if (skewedToHelperMappingHistory.keySet.contains(sortedWorkers(i))) {
          // worker has been previously paired with some helper and state migration has been done.
          // So, that helper will be used again.
          if (
            passSkewTest(
              sortedWorkers(i),
              skewedToHelperMappingHistory(sortedWorkers(i)),
              loads,
              Constants.reshapeEtaThreshold,
              Constants.reshapeTauThreshold
            )
          ) {
            retPairs.append(
              (sortedWorkers(i), skewedToHelperMappingHistory(sortedWorkers(i)), false)
            )
          }
        } else if (i > 0) {
          breakable {
            for (j <- 0 to i - 1) {
              if (
                isEligibleForHelper(sortedWorkers(j)) && passSkewTest(
                  sortedWorkers(i),
                  sortedWorkers(j),
                  loads,
                  Constants.reshapeEtaThreshold,
                  Constants.reshapeTauThreshold
                )
              ) {
                // this is the first time this skewed and helper workers are undergoing mitigation
                retPairs.append((sortedWorkers(i), sortedWorkers(j), true))
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

      workflow.getAllOperators.foreach(opConfig => {
        if (opConfig.isInstanceOf[HashJoinOpExecConfig[Any]]) {
          // Skew handling is only for hash-join operator for now.
          // Step 1: Find the skewed and helper worker that need first phase.
          val skewedAndHelperPairsForFirstPhase =
            getSkewedAndHelperWorkersEligibleForFirstPhase(opConfig.workerToWorkloadInfo)
          skewedAndHelperPairsForFirstPhase.foreach(skewedAndHelper =>
            logger.info(
              s"Reshape:First phase- Skewed ${skewedAndHelper._1.toString()} :: Helper ${skewedAndHelper._2
                .toString()} - Replication required: ${skewedAndHelper._3.toString()}"
            )
          )

          // Step 2: Do state transfer from the skewed to the helper worker if needed
          val stateMigrateFutures = new ArrayBuffer[Future[Boolean]]()
          skewedAndHelperPairsForFirstPhase.foreach(skewedAndHelper => {
            if (skewedAndHelper._3) {
              // transfer state from skewed to helper if it has not been transferred before
              stateMigrateFutures.append(
                send(SendImmutableState(skewedAndHelper._2), skewedAndHelper._1)
              )
            } else {
              stateMigrateFutures.append(Future.True)
            }
          })

          // Step 3: Start first phase for those pairs that transferred state successfully.
          val allPairsFirstPhaseFutures = new ArrayBuffer[Future[Seq[Boolean]]]()
          val prevWorkerLayer = getPreviousWorkerLayer(opConfig.getOperatorIdentity, workflow)
          firstPhaseRequestsFinished = false
          for (i <- 0 to skewedAndHelperPairsForFirstPhase.size - 1) {
            val currSkewedWorker = skewedAndHelperPairsForFirstPhase(i)._1
            val currHelperWorker = skewedAndHelperPairsForFirstPhase(i)._2
            stateMigrateFutures(i).map(stateTransferDone => {
              if (stateTransferDone) {
                skewedToHelperMappingHistory(currSkewedWorker) =
                  currHelperWorker // record mapping now that state transfer is done

                allPairsFirstPhaseFutures.append(
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
                  })
                )
              }
            })
          }
          Future
            .collect(allPairsFirstPhaseFutures)
            .onSuccess(_ => firstPhaseRequestsFinished = true)

          // Step 4: Start second phase for pairs where helpers that have caught up with skewed
          secondPhaseRequestsFinished = false
          val skewedAndHelperPairsForSecondPhase =
            getSkewedAndFreeWorkersEligibleForSecondPhase(opConfig.workerToWorkloadInfo)
          skewedAndHelperPairsForSecondPhase.foreach(skewedAndHelper =>
            logger.info(
              s"Reshape: Second phase - Skewed ${skewedAndHelper._1.toString()} :: Helper ${skewedAndHelper._2
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
            .onSuccess(_ => secondPhaseRequestsFinished = true)

          // Step 5: Pause mitigation for pairs where helpers have become overloaded
          pauseMitigationRequestsFinished = false
          val skewedAndHelperPairsForPauseMitigationPhase =
            getSkewedAndFreeWorkersEligibleForPauseMitigationPhase(opConfig.workerToWorkloadInfo)
          skewedAndHelperPairsForPauseMitigationPhase.foreach(skewedAndHelper =>
            logger.info(
              s"Reshape: Pause Mitigation phase - Skewed ${skewedAndHelper._1.toString()} :: Helper ${skewedAndHelper._2
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
            .onSuccess(_ => pauseMitigationRequestsFinished = true)

        }
      })
      previousSkewDetectionCallFinished = true
      Future.Done
    }
  })
}
