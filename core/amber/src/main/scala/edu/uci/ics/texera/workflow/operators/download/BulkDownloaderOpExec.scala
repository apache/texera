package edu.uci.ics.texera.workflow.operators.download

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.web.resource.dashboard.user.file.UserFileResource
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.source.fetcher.URLFetchUtil.getInputStreamFromURL

import java.net.URL
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.mutable

class BulkDownloaderOpExec(
    workflowContext: WorkflowContext,
    urlAttribute: String
) extends OperatorExecutor {

  private val downloading = new mutable.Queue[Future[TupleLike]]()
  private class DownloadResultIterator(blocking: Boolean) extends Iterator[TupleLike] {
    override def hasNext: Boolean = {
      if (downloading.isEmpty) {
        return false
      }
      if (blocking) {
        Await.result(downloading.head, 5.seconds)
      }
      downloading.head.isCompleted
    }

    override def next(): TupleLike = {
      downloading.dequeue().value.get.get
    }
  }

  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[TupleLike] = {
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

  private def downloadTuple(tuple: Tuple): TupleLike = {
    TupleLike(tuple.getFields ++ Seq(downloadUrl(tuple.getField(urlAttribute))))
  }

  private def downloadUrl(url: String): String = {
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
