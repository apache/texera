package edu.uci.ics.amber.engine.architecture.deploysemantics.layer

import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.worker.{WorkerState, WorkerStatistics}

// TODO: remove redundant info
case class WorkerInfo(id: ActorVirtualIdentity, var state: WorkerState, var stats: WorkerStatistics)
