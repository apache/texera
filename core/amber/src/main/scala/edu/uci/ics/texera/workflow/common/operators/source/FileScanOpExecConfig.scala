package edu.uci.ics.texera.workflow.common.operators.source

import edu.uci.ics.amber.engine.common.ambertag.OperatorIdentifier
import edu.uci.ics.amber.engine.operators.OpExecConfig

abstract class FileScanOpExecConfig(tag: OperatorIdentifier) extends OpExecConfig(tag) {
  val totalBytes: Long = 0
}
