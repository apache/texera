package edu.uci.ics.amber.operator

import edu.uci.ics.amber.core.executor.OperatorExecutor

trait StateTransferFunc
    extends ((OperatorExecutor, OperatorExecutor) => Unit)
    with java.io.Serializable
