package edu.uci.ics.amber.engine.architecture.worker

import java.io.{OutputStream, PrintStream}
import scala.util.DynamicVariable

object ThreadLocalPrintStream {

  // A dynamic variable to hold the per-thread PrintStream
  private val threadLocalStream = new DynamicVariable[Option[PrintStream]](None)

  // Custom OutputStream that delegates to the thread-local PrintStream if set, otherwise to the default OutputStream
  class CustomOutputStream(default: OutputStream) extends OutputStream {
    override def write(b: Int): Unit = {
      threadLocalStream.value match {
        case Some(ps) => ps.write(b)
        case None     => default.write(b)
      }
    }

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      threadLocalStream.value match {
        case Some(ps) => ps.write(b, off, len)
        case None     => default.write(b, off, len)
      }
    }

    override def flush(): Unit = {
      threadLocalStream.value match {
        case Some(ps) => ps.flush()
        case None     => default.flush()
      }
    }

    override def close(): Unit = {
      threadLocalStream.value match {
        case Some(ps) => ps.close()
        case None     => default.close()
      }
    }
  }

  // Initialize the custom PrintStream
  def init(): Unit = {
    val customOut = new PrintStream(new CustomOutputStream(System.out), true) // autoFlush=true
    System.setOut(customOut)
  }

  // Function to execute a code block with a custom PrintStream for the current thread
  def withPrintStream[T](ps: PrintStream)(block: => T): T = {
    threadLocalStream.withValue(Some(ps)) {
      block
    }
  }
}
