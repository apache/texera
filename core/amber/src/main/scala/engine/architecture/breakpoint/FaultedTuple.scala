package engine.architecture.breakpoint

import engine.common.tuple.Tuple

class FaultedTuple(val tuple: Tuple, val id: Long, val isInput: Boolean = false)
