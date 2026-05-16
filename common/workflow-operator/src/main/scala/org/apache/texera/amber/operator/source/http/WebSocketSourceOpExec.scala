/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.operator.source.http

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.executor.SourceOperatorExecutor
import org.apache.texera.amber.core.tuple.TupleLike
import org.apache.texera.amber.operator.http.util.HttpClientFactory
import org.apache.texera.amber.util.JSONUtils.objectMapper

import java.net.URI
import java.net.http.WebSocket
import java.net.http.WebSocket.Listener
import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CompletionStage, LinkedBlockingQueue, TimeUnit}

class WebSocketSourceOpExec(descString: String)
    extends SourceOperatorExecutor
    with LazyLogging {
  private val desc: WebSocketSourceOpDesc =
    objectMapper.readValue(descString, classOf[WebSocketSourceOpDesc])

  // The end-of-stream marker. Reference identity is used to disambiguate from
  // any legitimate message that happens to contain the same characters.
  private val sentinel: String = new String("__WS_CLOSED_SENTINEL__")
  private val queue = new LinkedBlockingQueue[String]()
  private val framesReceived = new AtomicLong(0L)
  @volatile private var webSocket: WebSocket = _

  override def open(): Unit = {
    logger.info(
      s"[WebSocketSource] opening: url=${desc.wsUrl} subscribeMsgLen=${Option(desc.subscribeMessage).map(_.length).getOrElse(0)}"
    )
    val builder = HttpClientFactory.sharedClient.newWebSocketBuilder()
    Option(desc.headers).foreach { hs =>
      hs.forEach { kv =>
        if (kv != null && kv.key != null && kv.value != null) {
          builder.header(kv.key, kv.value)
        }
      }
    }

    val listener = new Listener {
      private val partial = new StringBuilder()

      override def onOpen(ws: WebSocket): Unit = {
        // Request unlimited messages up front so per-frame backpressure
        // bookkeeping cannot accidentally stop delivery.
        ws.request(Long.MaxValue)
        logger.info("[WebSocketSource] onOpen — requesting unlimited messages")
        if (desc.subscribeMessage != null && desc.subscribeMessage.nonEmpty) {
          val send = desc.subscribeMessage
          logger.info(s"[WebSocketSource] sending subscribe (${send.length} chars): $send")
          ws.sendText(send, true)
        } else {
          logger.info("[WebSocketSource] no subscribe message configured")
        }
      }

      override def onText(
          ws: WebSocket,
          data: CharSequence,
          last: Boolean
      ): CompletionStage[_] = {
        partial.append(data)
        if (last) {
          val msg = partial.toString
          partial.clear()
          val n = framesReceived.incrementAndGet()
          if (n <= 5 || n % 100 == 0) {
            val preview = msg.substring(0, math.min(120, msg.length))
            logger.info(s"[WebSocketSource] frame #$n (${msg.length} chars): $preview")
          }
          queue.offer(msg)
        }
        null
      }

      override def onClose(ws: WebSocket, statusCode: Int, reason: String): CompletionStage[_] = {
        logger.warn(
          s"[WebSocketSource] onClose status=$statusCode reason='$reason' frames=${framesReceived.get()}"
        )
        queue.offer(sentinel)
        null
      }

      override def onError(ws: WebSocket, error: Throwable): Unit = {
        logger.error(
          s"[WebSocketSource] onError frames=${framesReceived.get()}: ${error.getClass.getSimpleName}: ${error.getMessage}",
          error
        )
        queue.offer(sentinel)
      }
    }

    webSocket = builder
      .buildAsync(toUri(desc.wsUrl), listener)
      .toCompletableFuture
      .get(30, TimeUnit.SECONDS)
    logger.info("[WebSocketSource] handshake complete")
  }

  // java.net.URI is strict about a few characters (notably `@`) that are
  // legal in WebSocket URLs in practice — e.g. Binance stream names like
  // `btcusdt@trade`. Percent-encode the most common offenders so users can
  // paste the URL verbatim from the provider docs.
  private def toUri(raw: String): URI = {
    if (raw == null) {
      throw new IllegalArgumentException("WebSocket URL must not be null")
    }
    // Defensive: strip whitespace (users frequently paste with trailing
    // spaces/newlines) before percent-encoding characters that are legal in
    // practice but rejected by java.net.URI (notably `@`).
    val encoded = raw.trim.replace("@", "%40")
    URI.create(encoded)
  }

  override def produceTuple(): Iterator[TupleLike] = new Iterator[TupleLike] {
    private var pending: String = _

    override def hasNext: Boolean = {
      if (pending != null && (pending ne sentinel)) return true
      try {
        pending = queue.take()
      } catch {
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          return false
      }
      pending ne sentinel
    }

    override def next(): TupleLike = {
      if (pending == null || (pending eq sentinel)) {
        throw new NoSuchElementException("WebSocket source has no more messages")
      }
      val msg = pending
      pending = null
      TupleLike(msg, new Timestamp(System.currentTimeMillis()))
    }
  }

  override def close(): Unit = {
    Option(webSocket).foreach { ws =>
      try ws.sendClose(WebSocket.NORMAL_CLOSURE, "operator closed")
      catch { case _: Throwable => /* ignore */ }
    }
    queue.offer(sentinel)
  }
}
