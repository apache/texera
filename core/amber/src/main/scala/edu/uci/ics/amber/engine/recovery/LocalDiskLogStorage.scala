package edu.uci.ics.amber.engine.recovery

import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.ScalaKryoInstantiator
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.recovery.DataLogManager.{FromSender, IdentifierMapping}

import scala.collection.mutable

class LocalDiskLogStorage[T](logName: String) extends LogStorage[T](logName) {

  private val path = Paths.get(s"./logs/$logName.logfile")
  Files.createDirectories(path.getParent)
  private val kryoInit = new ScalaKryoInstantiator
  kryoInit.setRegistrationRequired(false)
  private val kryo = kryoInit.newKryo()
  kryo.register(WorkflowControlMessage.getClass)
  kryo.register(IdentifierMapping.getClass)
  kryo.register(FromSender.getClass)
  private lazy val controlSerializer = new Output(
    Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
  )

  override def load(): Iterable[T] = {
    // read file from disk
    if (!Files.exists(path)) {
      return Seq.empty
    }
    val input = new Input(Files.newInputStream(path))
    var flag = true
    val buf = mutable.ArrayBuffer.empty[T]
    while (flag) {
      try {
        val message = kryo.readClassAndObject(input).asInstanceOf[T]
        buf.append(message)
      } catch {
        case e: KryoException =>
          input.close()
          flag = false
        case other =>
          throw other
      }
    }
    buf
  }

  override def persistElement(elem: T): Unit = {
    try {
      kryo.writeClassAndObject(controlSerializer, elem)
    } finally {
      controlSerializer.flush()
    }
  }

  override def clear(): Unit = {
    //delete file
    if (Files.exists(path)) {
      Files.delete(path)
    }
  }
}
