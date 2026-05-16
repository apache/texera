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

package org.apache.texera.amber.operator.aiagent

import java.net.{Inet4Address, Inet6Address, InetAddress, URI}

object AIAgentUrlSafety {
  private val AllowedSchemes = Set("http", "https")

  def validatePublicHttpUrl(rawUrl: String): URI = {
    require(rawUrl != null && rawUrl.trim.nonEmpty, "url is required")
    val uri = URI.create(rawUrl.trim)
    val scheme = Option(uri.getScheme).map(_.toLowerCase).getOrElse("")
    require(AllowedSchemes.contains(scheme), "Only http(s) URLs are allowed")
    val host = Option(uri.getHost).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException("URL host is required")
    }
    require(!isLocalHostName(host), s"Private or local URL hosts are not allowed: $host")
    val addresses = InetAddress.getAllByName(host)
    require(addresses.nonEmpty, s"Could not resolve URL host: $host")
    addresses.foreach { address =>
      require(!isPrivateAddress(address), s"Private or local URL hosts are not allowed: $host")
    }
    uri
  }

  private def isLocalHostName(host: String): Boolean = {
    val normalized = host.stripSuffix(".").toLowerCase
    normalized == "localhost" || normalized.endsWith(".localhost")
  }

  private def isPrivateAddress(address: InetAddress): Boolean =
    address.isAnyLocalAddress ||
      address.isLoopbackAddress ||
      address.isLinkLocalAddress ||
      address.isSiteLocalAddress ||
      address.isMulticastAddress ||
      isCarrierGradeNat(address) ||
      isUniqueLocalIpv6(address)

  private def isCarrierGradeNat(address: InetAddress): Boolean =
    address match {
      case ipv4: Inet4Address =>
        val bytes = ipv4.getAddress.map(_ & 0xff)
        bytes(0) == 100 && bytes(1) >= 64 && bytes(1) <= 127
      case _ => false
    }

  private def isUniqueLocalIpv6(address: InetAddress): Boolean =
    address match {
      case ipv6: Inet6Address =>
        val first = ipv6.getAddress.head & 0xff
        (first & 0xfe) == 0xfc
      case _ => false
    }
}
