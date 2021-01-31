package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.architecture.worker.WorkerStatistics
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager.WorkerState
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

case class WorkerInfo(id: ActorVirtualIdentity, var state: WorkerState, var stats: WorkerStatistics)
