package edu.uci.ics.amber.storage.result

import edu.uci.ics.amber.core.tuple.{Schema, Tuple}

trait SinkStorageReader {
  var schema: Schema = _
  def getSchema: Schema

  def setSchema(schema: Schema): Unit

  def getAll: Iterable[Tuple]

  def getRange(from: Int, to: Int): Iterable[Tuple]

  def getAllAfter(offset: Int): Iterable[Tuple]

  def getCount: Long

  def getStorageWriter: SinkStorageWriter

  def clear(): Unit

  def getAllFields(): Array[Array[String]] = {
    Array.ofDim[String](0, 0)
  }

  def getNumericColStats(fields: Iterable[String]): Map[String, Map[String, Any]] = {
    Map[String, Map[String, Any]]()
  }

  def getDateColStats(fields: Iterable[String]): Map[String, Map[String, Any]] = {
    Map[String, Map[String, Any]]()
  }

  def getCatColStats(fields: Iterable[String]): Map[String, Map[String, Any]] = {
    Map[String, Map[String, Any]]()
  }
}
