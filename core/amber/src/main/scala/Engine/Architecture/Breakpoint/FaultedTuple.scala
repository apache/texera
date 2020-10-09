package Engine.Architecture.Breakpoint

import Engine.Common.tuple.Tuple

class FaultedTuple(val tuple: Tuple, val id: Long, val isInput: Boolean = false)
