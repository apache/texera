package edu.uci.ics.amber.engine.common.statetransition

import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState._

// The following pattern is a good practice of enum in scala
// We've always used this pattern in the codebase
// https://nrinaudo.github.io/scala-best-practices/definitions/adt.html
// https://nrinaudo.github.io/scala-best-practices/adts/product_with_serializable.html

class WorkerStateManager(initialState: WorkerState = UNINITIALIZED)
    extends StateManager[WorkerState](
      Map(
        UNINITIALIZED -> Set(READY),
        READY -> Set(PAUSED, RUNNING, COMPLETED),
        RUNNING -> Set(PAUSED, COMPLETED),
        PAUSED -> Set(RUNNING),
        COMPLETED -> Set()
      ),
      initialState
    ) {}
