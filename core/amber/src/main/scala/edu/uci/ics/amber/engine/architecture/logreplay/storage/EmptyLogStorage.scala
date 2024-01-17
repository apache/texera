package edu.uci.ics.amber.engine.architecture.logreplay.storage

import edu.uci.ics.amber.engine.architecture.logreplay.storage.ReplayLogStorage.{
  ReplayLogReader,
  ReplayLogWriter
}
import org.apache.commons.io.input.NullInputStream
import org.apache.hadoop.io.IOUtils.NullOutputStream

import java.io.{DataInputStream, DataOutputStream}

class EmptyLogStorage extends ReplayLogStorage {
  override def getWriter(logFileName: String): ReplayLogWriter = {
    new ReplayLogWriter(
      new DataOutputStream(
        new NullOutputStream()
      )
    )
  }

  override def getReader(logFileName: String): ReplayLogReader = {
    new ReplayLogReader(() =>
      new DataInputStream(
        new NullInputStream()
      )
    )
  }

  override def deleteStorage(): Unit = {
    // empty
  }

}
