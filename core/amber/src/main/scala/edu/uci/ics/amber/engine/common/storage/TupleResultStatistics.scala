package edu.uci.ics.amber.engine.common.storage

import scala.collection.immutable.Map

trait TupleResultStatistics {
  // a map from field name to stats
  def getNumericColStats: Map[String, Map[String, Any]]

  def getDateColStats: Map[String, Map[String, Any]]

  def getCategoricalStats: Map[String, Map[String, Any]]
}
