package edu.uci.ics.amber.engine.common.statetransition

import edu.uci.ics.amber.engine.common.worker.WorkerState
import edu.uci.ics.amber.engine.common.worker.WorkerState._

// The following pattern is a good practice of enum in scala
// We've always used this pattern in the codebase
// https://nrinaudo.github.io/scala-best-practices/definitions/adt.html
// https://nrinaudo.github.io/scala-best-practices/adts/product_with_serializable.html

class WorkerStateManager(initialState: WorkerState = Uninitialized)
    extends StateManager[WorkerState](
      Map(
        Uninitialized -> Set(Ready),
        Ready -> Set(Paused, Running),
        Running -> Set(Paused, Completed),
        Paused -> Set(Running),
        Completed -> Set()
      ),
      initialState
    ) {}
