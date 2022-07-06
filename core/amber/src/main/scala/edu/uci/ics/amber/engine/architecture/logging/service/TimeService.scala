package edu.uci.ics.amber.engine.architecture.logging.service

import edu.uci.ics.amber.engine.architecture.logging.{LogManager, TimeStamp}

class TimeService(logManager: LogManager) {

  def getCurrentTime: Long = {
    // Add recovery logic later
    val time = System.currentTimeMillis()
    logManager.logInMemDeterminant(TimeStamp(time))
    time
  }

}
