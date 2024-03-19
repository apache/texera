package edu.uci.ics.amber.engine.common.storage

import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.{KryoBase, KryoPool, KryoSerializer, ScalaKryoInstantiator}
import edu.uci.ics.amber.engine.architecture.logreplay.{
  MessageContent,
  ProcessingStep,
  ReplayLogRecord
}
import SequentialRecordStorage.{SequentialRecordReader, SequentialRecordWriter}
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.ControlCommandV2Message.SealedValue.QueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}

import java.io.{DataInputStream, DataOutputStream}
import java.net.URI
import scala.collection.mutable.ArrayBuffer

object SequentialRecordStorage {
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
  def fetchAllRecords[T >: Null <: AnyRef](
      storage: SequentialRecordStorage[T],
      logFileName: String
  ): Iterable[T] = {
    val reader = storage.getReader(logFileName)
    val recordIter = reader.mkRecordIterator()
    val buffer = new ArrayBuffer[T]()
    while (recordIter.hasNext) {
      buffer.append(recordIter.next())
    }
    buffer
  }

  class SequentialRecordWriter[T >: Null <: AnyRef](outputStream: DataOutputStream) {
    lazy val output = new Output(outputStream)
    def writeRecord(obj: T): Unit = {
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

  class SequentialRecordReader[T >: Null <: AnyRef](inputStreamGen: () => DataInputStream) {
    def mkRecordIterator(): Iterator[T] = {
      lazy val input = new Input(inputStreamGen())
      new Iterator[T] {
        var record: T = internalNext()
        private def internalNext(): T = {
          try {
            val len = input.readInt()
            val bytes = input.readBytes(len)
            kryoPool.fromBytes(bytes).asInstanceOf[T]
          } catch {
            case e: Throwable =>
              input.close()
              null
          }
        }
        override def next(): T = {
          val currentRecord = record
          record = internalNext()
          currentRecord
        }
        override def hasNext: Boolean = record != null
      }
    }
  }

  def getStorage[T >: Null <: AnyRef](storageLocation: Option[URI]): SequentialRecordStorage[T] = {
    storageLocation match {
      case Some(location) =>
        if (location.getScheme.toLowerCase == "hdfs") {
          new HDFSRecordStorage(location) // hdfs lib supports r/w operations
        } else {
          new VFSRecordStorage(location)
        }
      case None => new EmptyRecordStorage()
    }
  }
}

/**
  * Sequential record storage is designed to do read/write for sequential generic data. It represents
  * a one-level folder (no nesting) which contains a list of files. Files are identified by a unique
  * file name string.
  *
  * Key Features:
  *   - Allows for the sequential writing and reading of records of a generic type `T`.
  *     It utilizes Kryo serialization for efficient binary storage of records.
  *   - The class assumes a sequential access pattern to the data. It is not optimized for random
  *     access or querying specific records without reading sequentially.
  * Usage:
  *   - To use `SequentialRecordStorage`, one must extend this abstract class and implement the
  *     methods for creating record readers and writers. Implementations can customize how and
  *     where the data is stored and retrieved.
  *   - The `SequentialRecordWriter` and `SequentialRecordReader` inner classes provide the
  *     functionality for writing to and reading from the storage.
  *
  * @tparam T The type of records that this storage system will handle.
  */
abstract class SequentialRecordStorage[T >: Null <: AnyRef] {

  def getWriter(fileName: String): SequentialRecordWriter[T]

  def getReader(fileName: String): SequentialRecordReader[T]

  def deleteStorage(): Unit

  def containsFolder(folderName: String): Boolean
}
