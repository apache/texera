package edu.uci.ics.amber.engine.common

import scala.annotation.elidable
import scala.annotation.elidable._

object ElidableStatement {

  @elidable(FINEST) def finest(operations: => Unit): Unit = operations

  @elidable(FINER) def finer(operations: => Unit): Unit = operations

  @elidable(FINE) def fine(operations: => Unit): Unit = operations

  @elidable(INFO) def info(operations: => Unit): Unit = operations
}
