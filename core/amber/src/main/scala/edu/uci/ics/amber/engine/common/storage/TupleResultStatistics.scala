package edu.uci.ics.amber.engine.common.storage

trait TupleResultStatistics {
  // a map from numerical fields to stats
  def getNumericColStats: Map[String, Map[String, Any]]

  // a map from date fields to stats
  def getDateColStats: Map[String, Map[String, Any]]

  // a map from categorical fields to stats
  def getCategoricalStats: Map[String, Map[String, Any]]
}
