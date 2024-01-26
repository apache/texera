package edu.uci.ics.texera.workflow.operators.download

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}
import edu.uci.ics.texera.workflow.operators.source.fetcher.URLFetchUtil.getInputStreamFromURL

import java.net.URL
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable

class BulkDownloaderOpExec(
    val workflowContext: WorkflowContext,
    val urlAttribute: String,
    val resultAttribute: String,
    val outputSchema: Schema
) extends OperatorExecutor {
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

    val builder = Tuple.newBuilder(outputSchema)
    outputSchema.getAttributes
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
    try {
      Await.result(
        Future {
          val urlObj = new URL(url)
          val input = getInputStreamFromURL(urlObj)
          input match {
            case Some(contentStream) =>
              if (contentStream.available() > 0) {
                val filename =
                  s"w${workflowContext.workflowId.id}-e${workflowContext.executionId.id}-${urlObj.getHost
                    .replace(".", "")}.download"
                UserFileResource
                  .saveFile(
                    workflowContext.userId.get,
                    filename,
                    contentStream,
                    s"downloaded by execution ${workflowContext.executionId.id} of workflow ${workflowContext.workflowId.id}. Original URL = $url"
                  )
                filename
              } else {
                throw new RuntimeException(s"content is not available for $url")
              }
            case None =>
              throw new RuntimeException(s"fetch content failed for $url")
          }
        },
        5.seconds
      )
    } catch {
      case throwable: Throwable => s"Failed: ${throwable.getMessage}"
    }
  }

}
