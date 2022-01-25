package edu.uci.ics.texera.workflow.operators.sink.storage

import edu.uci.ics.texera.workflow.common.tuple.Tuple

trait SinkStorageWriter {

  def open(): Unit

  def close(): Unit

  def putOne(tuple: Tuple): Unit

  def removeOne(tuple: Tuple): Unit

}
