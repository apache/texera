package edu.uci.ics.amber.engine.architecture.logging.storage

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.ScalaKryoInstantiator
import edu.uci.ics.amber.engine.architecture.logging.InMemDeterminant
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.{DeterminantLogReader, DeterminantLogWriter}

import java.io.{DataInputStream, DataOutputStream}

object DeterminantLogStorage {
  val instantiator = new ScalaKryoInstantiator
  instantiator.setRegistrationRequired(false)

  abstract class DeterminantLogWriter {
    protected val outputStream: DataOutputStream
    lazy val output = new Output(outputStream)
    lazy val kryo = instantiator.newKryo()
    def writeLogRecord(obj: InMemDeterminant): Unit = {
      kryo.writeClassAndObject(output, obj)
    }
    def flush(): Unit = {
      output.flush()
    }
    def close(): Unit = {
      output.close()
    }
  }

  abstract class DeterminantLogReader {
    protected val inputStream: DataInputStream
    lazy val input = new Input(inputStream)
    lazy val kryo = instantiator.newKryo()
    def readLogRecord():InMemDeterminant = {
      kryo.readClassAndObject(input).asInstanceOf[InMemDeterminant]
    }
    def close(): Unit = {
      input.close()
    }
  }
}

abstract class DeterminantLogStorage {

  def getWriter(attempt: Int): DeterminantLogWriter

  def getReader(attempt: Int): DeterminantLogReader

  def deleteLog(attempt: Int): Unit

}
