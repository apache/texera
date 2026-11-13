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

package org.apache.texera.common.util

import java.net.URI
import scala.util.Try

/**
  * Profile-picture URLs as an identity provider supplies them.
  *
  * `"user".avatar` used to hold only the last path segment of Google's `picture` claim, with the
  * frontend rebuilding `https://lh3.googleusercontent.com/a/<fragment>` around it — which made
  * the stored value meaningless for any other provider. The complete URL is stored instead, so
  * the column is provider-neutral; the cost is that a provider now picks a URL the browser will
  * fetch, which is what the allowlist below bounds.
  */
object AvatarUtil {

  /** An exact host or any subdomain of one of these may serve an avatar. */
  private val ALLOWED_HOST_SUFFIXES: Set[String] = Set(
    "googleusercontent.com"
  )

  private[util] def isAllowedHost(host: String): Boolean = {
    if (host == null || host.isEmpty) return false
    val lower = host.toLowerCase
    ALLOWED_HOST_SUFFIXES.exists(suffix => lower == suffix || lower.endsWith("." + suffix))
  }

  /**
    * The avatar URL to persist, or `None` to leave the stored value alone. `None` also covers a
    * provider that supplies no picture, so "no avatar" is a single case for callers: the user
    * keeps whatever is on file and falls back to the initials avatar if that is nothing.
    *
    * Anything that is not an http(s) URL on an allowlisted host is dropped rather than rejected —
    * a surprising avatar is not a reason to deny someone a login.
    */
  def sanitize(url: Option[String]): Option[String] =
    url.map(_.trim).filter(_.nonEmpty).filter { candidate =>
      Try(URI.create(candidate)).toOption
        .filter { uri =>
          val scheme = Option(uri.getScheme).map(_.toLowerCase)
          scheme.contains("http") || scheme.contains("https")
        }
        .flatMap(uri => Option(uri.getHost))
        .exists(isAllowedHost)
    }
}
