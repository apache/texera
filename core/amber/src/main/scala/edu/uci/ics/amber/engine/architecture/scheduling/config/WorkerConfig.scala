package edu.uci.ics.amber.engine.architecture.scheduling.config

import edu.uci.ics.amber.engine.common.AmberConfig

case class WorkerConfig(
    logStorageType: String = AmberConfig.faultToleranceLogRootFolder,
    replayTo: Option[Long] = None
)
