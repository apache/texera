package edu.uci.ics.amber.engine.architecture.logreplay.storage

import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.{KryoBase, KryoPool, KryoSerializer, ScalaKryoInstantiator}
import edu.uci.ics.amber.engine.architecture.logreplay.{
  MessageContent,
  ProcessingStep,
  ReplayLogRecord
}
import edu.uci.ics.amber.engine.architecture.logreplay.storage.ReplayLogStorage.{
  ReplayLogReader,
  ReplayLogWriter
}
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.ControlCommandV2Message.SealedValue.QueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}

import java.io.{DataInputStream, DataOutputStream}
import java.net.URI
import scala.collection.mutable.ArrayBuffer

object ReplayLogStorage {
  private val kryoPool = {
    val r = KryoSerializer.registerAll
    val ki = new ScalaKryoInstantiator {
      override def newKryo(): KryoBase = {
        val kryo = super.newKryo()
        kryo.register(classOf[ReplayLogRecord])
        kryo.register(classOf[MessageContent])
        kryo.register(classOf[ProcessingStep])
        kryo.register(classOf[ControlInvocation])
        kryo.register(classOf[WorkerState])
        kryo.register(classOf[ReturnInvocation])
        kryo.register(classOf[QueryStatistics])
        kryo
      }
    }.withRegistrar(r)
    KryoPool.withByteArrayOutputStream(Runtime.getRuntime.availableProcessors * 2, ki)
  }

  // For debugging purpose only
  def fetchAllLogRecords(
      storage: ReplayLogStorage,
      logFileName: String
  ): Iterable[ReplayLogRecord] = {
    val reader = storage.getReader(logFileName)
    val recordIter = reader.mkLogRecordIterator()
    val buffer = new ArrayBuffer[ReplayLogRecord]()
    while (recordIter.hasNext) {
      buffer.append(recordIter.next())
    }
    buffer
  }

  class ReplayLogWriter(outputStream: DataOutputStream) {
    lazy val output = new Output(outputStream)
    def writeLogRecord(obj: ReplayLogRecord): Unit = {
      val bytes = kryoPool.toBytesWithClass(obj)
      output.writeInt(bytes.length)
      output.write(bytes)
    }
    def flush(): Unit = {
      output.flush()
    }
    def close(): Unit = {
      output.close()
    }
  }

  class ReplayLogReader(inputStreamGen: () => DataInputStream) {
    def mkLogRecordIterator(): Iterator[ReplayLogRecord] = {
      lazy val input = new Input(inputStreamGen())
      new Iterator[ReplayLogRecord] {
        var record: ReplayLogRecord = internalNext()
        private def internalNext(): ReplayLogRecord = {
          try {
            val len = input.readInt()
            val bytes = input.readBytes(len)
            kryoPool.fromBytes(bytes).asInstanceOf[ReplayLogRecord]
          } catch {
            case e: Throwable =>
              input.close()
              null
          }
        }
        override def next(): ReplayLogRecord = {
          val currentRecord = record
          record = internalNext()
          currentRecord
        }
        override def hasNext: Boolean = record != null
      }
    }
  }

  def getLogStorage(logFolderURI: Option[URI]): ReplayLogStorage = {
    logFolderURI match {
      case Some(value) => new URILogStorage(value)
      case None        => new EmptyLogStorage()
    }
  }
}

abstract class ReplayLogStorage {

  def getWriter(logFileName: String): ReplayLogWriter

  def getReader(logFileName: String): ReplayLogReader

  def deleteStorage(): Unit
}
