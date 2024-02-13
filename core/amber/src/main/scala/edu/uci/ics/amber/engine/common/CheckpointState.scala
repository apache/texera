package edu.uci.ics.amber.engine.common

import scala.collection.mutable

class CheckpointState {

  private val states = new mutable.HashMap[String, SerializedState]()

  def save[T <: Any](key: String, state: T): Unit = {
    states(key) = SerializedState.fromObject(state.asInstanceOf[AnyRef], AmberUtils.serde)
  }

  def has(key: String): Boolean = {
    states.contains(key)
  }

  def load[T <: Any](key: String): T = {
    if (states.contains(key)) {
      states(key).toObject(AmberUtils.serde).asInstanceOf[T]
    } else {
      throw new RuntimeException(s"no state saved for key = $key")
    }
  }

  def size(): Long = {
    states.filter(_._2 != null).map(_._2.size()).sum
  }

}
