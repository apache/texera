package edu.uci.ics.texera.workflow.common.operators.source

import edu.uci.ics.amber.engine.common.ambertag.OperatorIdentifier
import edu.uci.ics.amber.engine.operators.OpExecConfig

abstract class SourceOpExecConfig(tag: OperatorIdentifier) extends OpExecConfig(tag) {
  // Right now we only set size in CSV and HDFS file scans. We don't have a way to
  // set size for source like MySQL scan operator. So, totalSize for them will be 0.
  val totalSize: Long = 0
}
