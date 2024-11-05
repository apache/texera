package edu.uci.ics.amber.operator.download

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.operator.source.fetcher.URLFetchUtil.getInputStreamFromURL

import java.net.URL
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

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

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    downloading.enqueue(Future { downloadTuple(tuple) })
    new DownloadResultIterator(false)
  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    new DownloadResultIterator(true)
  }

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
