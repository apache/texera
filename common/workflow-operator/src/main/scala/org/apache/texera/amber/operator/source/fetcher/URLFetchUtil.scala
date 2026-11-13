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

package org.apache.texera.amber.operator.source.fetcher

import java.io.InputStream
import java.net.URL
import org.apache.texera.common.util.RetryUtil

import scala.util.Try

object URLFetchUtil {
  def getInputStreamFromURL(urlObj: URL, retries: Int = 5): Option[InputStream] =
    getInputStreamFromURL(urlObj, retries, initialDelayMillis = 200L, sleep = Thread.sleep)

  private[fetcher] def getInputStreamFromURL(
      urlObj: URL,
      retries: Int,
      initialDelayMillis: Long,
      sleep: Long => Unit
  ): Option[InputStream] = {
    if (retries <= 0) {
      None
    } else {
      Try {
        RetryUtil.withBackoff(
          description = s"fetch ${urlObj.toExternalForm}",
          maxAttempts = retries,
          initialDelayMillis = initialDelayMillis,
          onRetry = _ => (),
          sleep = sleep
        ) {
          val request = urlObj.openConnection()
          request.setRequestProperty("User-Agent", RandomUserAgent.getRandomUserAgent)
          request.getInputStream
        }
      }.toOption
    }
  }
}
