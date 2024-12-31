package edu.uci.ics.amber.operator.source

import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.operator.LogicalOp

abstract class SourceOperatorDescriptor extends LogicalOp {

  def sourceSchema(): Schema
}
