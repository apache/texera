package edu.uci.ics.amber.engine.architecture.logging.storage

import com.esotericsoftware.kryo.Kryo
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogWriter

import java.io.InputStream

object DeterminantLogStorage {
  abstract class DeterminantLogWriter {
    val ser = new Kryo()
    def writeLogRecord(obj: AnyRef): Unit
    def flush(): Unit
    def close(): Unit
  }
}

abstract class DeterminantLogStorage {

  def getWriter: DeterminantLogWriter

  def deleteLog(): Unit

}
