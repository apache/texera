package Engine.Architecture.Breakpoint

import Engine.Common.AmberTuple.Tuple

class FaultedTuple(val tuple: Tuple, val id: Long, val isInput: Boolean = false)
