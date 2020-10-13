package edu.uci.ics.amber.engine.architecture.breakpoint

import edu.uci.ics.amber.engine.common.tuple.Tuple

class FaultedTuple(val tuple: Tuple, val id: Long, val isInput: Boolean = false)
