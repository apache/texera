package edu.uci.ics.amber.engine.architecture.logging.storage

import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.ScalaKryoInstantiator
import edu.uci.ics.amber.engine.architecture.logging.InMemDeterminant
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.{
  DeterminantLogReader,
  DeterminantLogWriter
}
import edu.uci.ics.amber.engine.common.AmberUtils

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
    def readLogRecord(): InMemDeterminant = {
      try{
        kryo.readClassAndObject(input).asInstanceOf[InMemDeterminant]
      }catch{
        case e:Exception =>
          null
      }
    }
    def close(): Unit = {
      input.close()
    }
  }

  def getLogStorage(name: String): DeterminantLogStorage = {
    val enabledLogging: Boolean =
      AmberUtils.amberConfig.getBoolean("fault-tolerance.enable-determinant-logging")
    val storageType: String =
      AmberUtils.amberConfig.getString("fault-tolerance.log-storage-type")
    if (enabledLogging) {
      storageType match {
        case "local" => new LocalFSLogStorage(name)
        case "hdfs" =>
          val hdfsIP: String =
            AmberUtils.amberConfig.getString("fault-tolerance.hdfs-storage.address")
          new HDFSLogStorage(name, hdfsIP)
        case other => throw new RuntimeException("Cannot support log storage type of " + other)
      }
    } else {
      new EmptyLogStorage()
    }
  }

}

abstract class DeterminantLogStorage {

  def getWriter(isTempLog: Boolean): DeterminantLogWriter

  def getReader: DeterminantLogReader

  def deleteLog(): Unit

  def swapTempLog(): Unit

}
