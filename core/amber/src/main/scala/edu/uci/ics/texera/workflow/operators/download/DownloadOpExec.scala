package edu.uci.ics.texera.workflow.operators.download

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo}

import java.io.{BufferedWriter, File, FileWriter}
import java.util.UUID
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.concurrent.duration._

class DownloadOpExec(
    val attributes: List[DownloadAttributeUnit],
    val operatorSchemaInfo: OperatorSchemaInfo
) extends OperatorExecutor {
  private val DOWNLOADS_PATH = new File(new File(".").getCanonicalPath).getParent + "/downloads"
  private val BATCH_SIZE = 10
  private val batchBuffer = collection.mutable.Buffer[Future[Tuple]]()

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) => {
        val futureTask = Future {
          downloadTuple(t)
        }
        batchBuffer += futureTask
        if (batchBuffer.size == BATCH_SIZE) processBatch else Iterator()
      }
      case Right(_) =>
        processBatch
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  def processBatch(): Iterator[Tuple] = {
    val combinedFuture = Future.sequence(batchBuffer)
    val output = Await.result(combinedFuture, Duration.Inf)
    batchBuffer.clear()
    output.iterator
  }

  def downloadTuple(tuple: Tuple) = {
    val builder = Tuple.newBuilder(operatorSchemaInfo.outputSchemas(0))

    for (attribute <- attributes) {
      builder.add(
        attribute.getUrlAttribute,
        tuple.getSchema.getAttribute(attribute.getUrlAttribute).getType,
        tuple.getField(attribute.getUrlAttribute)
      )
      builder.add(
        attribute.getResultAttribute,
        AttributeType.STRING,
        downloadUrl(tuple.getField(attribute.getUrlAttribute).toString)
      )
    }
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
        val data = Source.fromURL(url).mkString
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
