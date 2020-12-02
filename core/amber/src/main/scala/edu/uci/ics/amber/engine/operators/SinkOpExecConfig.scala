package edu.uci.ics.amber.engine.operators

import edu.uci.ics.amber.engine.common.ambertag.OperatorIdentifier

abstract class SinkOpExecConfig(tag: OperatorIdentifier) extends OpExecConfig(tag) {
  override def getInputNum(from: OperatorIdentifier): Int = 0
}
