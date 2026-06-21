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

package org.apache.texera.web

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import javax.servlet.{Filter, FilterChain, FilterConfig, ServletRequest, ServletResponse}

object StaticAssetCacheFilter {

  // `name.<hash>.ext`, capturing the hash segment.
  private val FingerprintedAsset = """.*\.([0-9a-f]{8,})\.[A-Za-z0-9]+""".r

  val ImmutableCacheControl = "public, max-age=31536000, immutable"
  val RevalidateCacheControl = "no-cache, must-revalidate"

  // Require a hex letter so all-numeric segments (dates, versions) aren't frozen as a hash.
  private def isFingerprinted(fileName: String): Boolean =
    fileName match {
      case FingerprintedAsset(hash) => hash.exists(c => c >= 'a' && c <= 'f')
      case _                        => false
    }

  // None for /api/*; immutable for fingerprinted assets; revalidate otherwise.
  def cacheControlFor(path: String): Option[String] = {
    if (path.startsWith("/api/")) None
    else if (isFingerprinted(path.substring(path.lastIndexOf('/') + 1)))
      Some(ImmutableCacheControl)
    else Some(RevalidateCacheControl)
  }
}

class StaticAssetCacheFilter extends Filter {
  override def init(filterConfig: FilterConfig): Unit = {}

  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      chain: FilterChain
  ): Unit = {
    (request, response) match {
      case (req: HttpServletRequest, resp: HttpServletResponse) =>
        StaticAssetCacheFilter
          .cacheControlFor(req.getRequestURI)
          .foreach(resp.setHeader("Cache-Control", _))
      case _ =>
    }
    chain.doFilter(request, response)
  }

  override def destroy(): Unit = {}
}
