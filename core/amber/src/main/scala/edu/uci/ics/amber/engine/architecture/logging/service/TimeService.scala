package edu.uci.ics.amber.engine.architecture.logging.service

import edu.uci.ics.amber.engine.architecture.logging.{DeterminantLogger, LogManager, TimeStamp}

class TimeService(determinantLogger: DeterminantLogger) {

  def getCurrentTime: Long = {
    // Add recovery logic later
    val time = System.currentTimeMillis()
    determinantLogger.logDeterminant(TimeStamp(time))
    time
  }

}
