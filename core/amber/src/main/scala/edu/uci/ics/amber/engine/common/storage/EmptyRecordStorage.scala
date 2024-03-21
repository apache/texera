package edu.uci.ics.amber.engine.common.storage

import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage.{
  SequentialRecordReader,
  SequentialRecordWriter
}
import org.apache.commons.io.input.NullInputStream
import org.apache.hadoop.io.IOUtils.NullOutputStream

import java.io.{DataInputStream, DataOutputStream}

class EmptyRecordStorage[T >: Null <: AnyRef] extends SequentialRecordStorage[T] {
  override def getWriter(fileName: String): SequentialRecordWriter[T] = {
    new SequentialRecordWriter(
      new DataOutputStream(
        new NullOutputStream()
      )
    )
  }

  override def getReader(fileName: String): SequentialRecordReader[T] = {
    new SequentialRecordReader(() =>
      new DataInputStream(
        new NullInputStream()
      )
    )
  }

  override def deleteStorage(): Unit = {
    // empty
  }

  override def containsFolder(folderName: String): Boolean = false
}
