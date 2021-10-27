package edu.uci.ics.texera.web

import scala.collection.mutable

object SessionStateManager {
  private val sessionIdToSessionState = new mutable.HashMap[String, SessionState]()

  def getState(sId: String): SessionState = {
    sessionIdToSessionState(sId)
  }

  def setState(sId: String, state: SessionState): Unit = {
    sessionIdToSessionState.put(sId, state)
  }

  def removeState(sId: String): Unit = {
    sessionIdToSessionState(sId).unsubscribe()
    sessionIdToSessionState.remove(sId)
  }
}
