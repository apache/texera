package edu.uci.ics.texera.workflow.operators.sink.storage

import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

trait SinkStorageReader {

  def getSchema: Schema

  def getAll: Iterable[Tuple]

  def getRange(from: Int, to: Int): Iterable[Tuple]

  def getAllAfter(offset: Int): Iterable[Tuple]

  def getCount: Long

  def getStorageWriter(): SinkStorageWriter

  def clear(): Unit
}
