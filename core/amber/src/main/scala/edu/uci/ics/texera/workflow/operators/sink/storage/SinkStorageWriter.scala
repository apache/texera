package edu.uci.ics.texera.workflow.operators.sink.storage

import edu.uci.ics.amber.engine.common.model.tuple.Tuple

trait SinkStorageWriter {
  def open(): Unit

  def close(): Unit

  def putOne(tuple: Tuple): Unit

  def removeOne(tuple: Tuple): Unit

}
