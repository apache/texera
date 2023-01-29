package edu.uci.ics.texera.workflow.operators.download

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo}

import java.io.{BufferedWriter, File, FileWriter}
import java.util
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.collection.mutable

class DownloadOpExec(
    val urlAttribute: String,
    val resultAttribute: String,
    val operatorSchemaInfo: OperatorSchemaInfo
) extends OperatorExecutor {
  private val DOWNLOADS_PATH =
    new File(new File(".").getCanonicalPath) + "/user-resources/downloads"
  private val downloading = new mutable.Queue[Future[Tuple]]()

  class DownloadResultIterator(blocking: Boolean) extends Iterator[Tuple] {
    override def hasNext: Boolean = {
      if (downloading.isEmpty) {
        return false
      }
      if (blocking) {
        Await.result(downloading.head, 5.seconds)
      }
      downloading.head.isCompleted
    }

    override def next(): Tuple = {
      downloading.dequeue().value.get.get
    }
  }

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        downloading.enqueue(Future { downloadTuple(t) })
        new DownloadResultIterator(false)
      case Right(_) =>
        new DownloadResultIterator(true)
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  def downloadTuple(tuple: Tuple): Tuple = {
    val builder = Tuple.newBuilder(operatorSchemaInfo.outputSchemas(0))

    operatorSchemaInfo
      .outputSchemas(0)
      .getAttributes
      .foreach(attr => {
        if (attr.getName == resultAttribute) {
          builder.add(
            resultAttribute,
            AttributeType.STRING,
            downloadUrl(tuple.getField(urlAttribute))
          )
        } else {
          builder.add(
            attr.getName,
            tuple.getSchema.getAttribute(attr.getName).getType,
            tuple.getField(attr.getName)
          )
        }
      })

    builder.build()
  }

  def downloadUrl(url: String): String = {
    var result: String = ""
    val future = Future {
      val directory = new File(DOWNLOADS_PATH)
      if (!directory.exists()) {
        directory.mkdir()
      }
      try {
        val source = Source.fromURL(url)
        val data = source.mkString
        source.close()
        val filePath = s"$DOWNLOADS_PATH/${UUID.randomUUID()}.txt"
        val file = new File(filePath)
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(data)
        bw.close()
        filePath
      } catch {
        case e: Exception => e.getMessage
      }
    }
    try {
      result = Await.result(future, 5.seconds)
    } catch {
      case e: Exception => result = e.getMessage
    }
    result
  }

}
