package edu.uci.ics.texera.workflow.common.operators.source

import edu.uci.ics.amber.engine.common.ambertag.OperatorIdentifier
import edu.uci.ics.amber.engine.operators.OpExecConfig

abstract class SourceOpExecConfig(tag: OperatorIdentifier) extends OpExecConfig(tag) {
  // totalSize is used by Join to decide which input stage to execute first. It checks the
  // input size of the Scan operator on both sides, labels the smaller size as build table
  // and starts it first.
  // Now that right now we only set size in CSV and HDFS file scans. We don't have a way to
  // set size for source like MySQL scan operator. So, totalSize for them will be 0 and they
  // will always be the build table.
  val totalSize: Long = 0
}
