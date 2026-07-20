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

package org.apache.texera.service.util

import com.typesafe.scalalogging.LazyLogging
import jakarta.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import org.apache.texera.common.config.StorageConfig
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.signer.AwsS3V4Signer
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams
import software.amazon.awssdk.http.{SdkHttpFullRequest, SdkHttpMethod}
import software.amazon.awssdk.regions.Region

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.jdk.CollectionConverters._

/**
  * Read-only, scope-enforcing, re-signing reverse proxy in front of the LakeFS S3
  * gateway. A computing-unit pod's GeeseFS mount talks to this servlet using a
  * short-lived [[MountSession]] credential (obtained from
  * [[org.apache.texera.service.resource.MountSessionResource]] with the pod's per-user
  * JWT). The servlet:
  *
  *   1. reads the session access key out of the incoming SigV4 `Authorization` header
  *      (the session key is the bearer capability; the pod-side signature is not
  *      re-validated, and no LakeFS credentials ever leave this service),
  *   2. enforces that the request stays within the session's `repository`@`commit`
  *      scope, so a session can never read another repository or commit, and
  *   3. re-signs the request with the global LakeFS credentials and forwards it to the
  *      LakeFS S3 gateway, streaming the response back.
  *
  * Because requests are forwarded verbatim, the proxy behaves identically to a direct
  * GeeseFS -> LakeFS-gateway mount, just re-authenticated. GeeseFS mounts read-only, so
  * only GET and HEAD are handled.
  */
class S3ProxyServlet extends HttpServlet with LazyLogging {

  // The LakeFS S3 gateway shares the LakeFS server address: the configured API endpoint
  // with the trailing /api/v1 suffix removed.
  private val gatewayEndpoint: URI =
    URI.create(StorageConfig.lakefsEndpoint.stripSuffix("/").stripSuffix("/api/v1"))

  private val lakefsCredentials =
    AwsBasicCredentials.create(StorageConfig.lakefsUsername, StorageConfig.lakefsPassword)

  // The S3-specific SigV4 signer adds and signs the x-amz-content-sha256 header (which
  // S3 / the LakeFS gateway require) and disables path double-encoding — both essential
  // for the signature to validate. The generic Aws4Signer omits x-amz-content-sha256.
  private val signer = AwsS3V4Signer.create()

  private val httpClient: HttpClient = HttpClient
    .newBuilder()
    .followRedirects(HttpClient.Redirect.NEVER)
    .connectTimeout(Duration.ofSeconds(10))
    .build()

  // Response headers worth passing back to GeeseFS; hop-by-hop and length/encoding
  // headers are recomputed by the servlet container, so they are intentionally omitted.
  private val forwardedResponseHeaderPrefixes = Seq("content-", "etag", "last-modified", "accept-ranges", "x-amz-")

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit =
    proxy(req, resp, SdkHttpMethod.GET, streamBody = true)

  override def doHead(req: HttpServletRequest, resp: HttpServletResponse): Unit =
    proxy(req, resp, SdkHttpMethod.HEAD, streamBody = false)

  private def proxy(
      req: HttpServletRequest,
      resp: HttpServletResponse,
      method: SdkHttpMethod,
      streamBody: Boolean
  ): Unit = {
    val now = System.currentTimeMillis()

    val accessKey = extractAccessKey(req.getHeader("Authorization"))
    val session = accessKey.flatMap(MountSessionStore.get(_, now))
    if (session.isEmpty) {
      // GeeseFS probes the bucket unauthenticated on mount, so this is expected noise.
      resp.sendError(HttpServletResponse.SC_FORBIDDEN, "invalid or expired mount session")
      return
    }

    if (!withinScope(req, session.get)) {
      logger.warn(
        s"mount session for ${session.get.repositoryName}@${session.get.commitHash} " +
          s"denied out-of-scope request: ${req.getRequestURI}"
      )
      resp.sendError(HttpServletResponse.SC_FORBIDDEN, "request outside mount session scope")
      return
    }

    try {
      writeResponse(forward(req, method), resp, streamBody)
    } catch {
      case e: Exception =>
        logger.error(s"error proxying ${req.getRequestURI} to LakeFS gateway", e)
        resp.sendError(HttpServletResponse.SC_BAD_GATEWAY, "upstream error")
    }
  }

  /**
    * Extract the access key id from an AWS `Authorization` header. GeeseFS signs with
    * either SigV4 (`AWS4-HMAC-SHA256 Credential=<accessKey>/<date>/...`) or, against a
    * plain-HTTP custom endpoint, SigV2 (`AWS <accessKey>:<signature>`); support both.
    * The pod-side signature itself is not re-validated — the session key is the bearer
    * capability — so only the access key id needs to be read out.
    */
  private[util] def extractAccessKey(authHeader: String): Option[String] = {
    Option(authHeader).flatMap { h =>
      "Credential=([^/,\\s]+)/".r
        .findFirstMatchIn(h)
        .map(_.group(1)) // SigV4
        .orElse("^AWS ([^:\\s]+):".r.findFirstMatchIn(h.trim).map(_.group(1))) // SigV2
    }
  }

  /**
    * True iff the request targets only the session's `repository`@`commit`. GeeseFS
    * addresses a `repo:commit` mount as bucket=`repo` with every key/list-prefix under
    * `commit/`, so we require: bucket == repository, object keys start with the commit,
    * and listings carry a prefix under the commit. Bucket-level metadata requests
    * (e.g. `?location`) carry no key/prefix and expose no data, so they are allowed.
    */
  private[util] def withinScope(req: HttpServletRequest, session: MountSession): Boolean = {
    val segments = req.getRequestURI.stripPrefix("/").split("/", 2)
    val bucket = java.net.URLDecoder.decode(segments(0), "UTF-8")
    if (bucket != session.repositoryName) return false

    val key = if (segments.length > 1) java.net.URLDecoder.decode(segments(1), "UTF-8") else ""
    if (key.nonEmpty) {
      // object request: must be inside the commit prefix
      return key == session.commitHash || key.startsWith(session.commitHash + "/")
    }

    // bucket-level request: a listing must be confined to the commit prefix
    val isListing =
      req.getParameter("list-type") != null ||
        req.getParameter("prefix") != null ||
        req.getParameter("marker") != null
    if (isListing) {
      val prefix = Option(req.getParameter("prefix")).getOrElse("")
      prefix.startsWith(session.commitHash)
    } else {
      // non-listing bucket metadata (location, versioning, HEAD bucket): no data exposure
      true
    }
  }

  /** Re-sign the request with LakeFS credentials and send it to the LakeFS gateway. */
  private def forward(
      req: HttpServletRequest,
      method: SdkHttpMethod
  ): HttpResponse[java.io.InputStream] = {
    val query = Option(req.getQueryString).map("?" + _).getOrElse("")
    val targetUri = URI.create(
      gatewayEndpoint.getScheme + "://" + gatewayEndpoint.getAuthority + req.getRequestURI + query
    )

    val builder = SdkHttpFullRequest
      .builder()
      .method(method)
      .uri(targetUri)
    // Range must be part of the signed request so the gateway accepts it.
    Option(req.getHeader("Range")).foreach(r => builder.putHeader("Range", r))

    val signed = signer.sign(
      builder.build(),
      AwsS3V4SignerParams
        .builder()
        .awsCredentials(lakefsCredentials)
        .signingName("s3")
        .signingRegion(Region.US_EAST_1)
        .build()
    )

    var outbound = HttpRequest
      .newBuilder()
      .uri(targetUri)
      .method(method.name(), HttpRequest.BodyPublishers.noBody())
    // Forward every signed header except Host, which the HTTP client sets itself to the
    // same value we signed (the gateway authority).
    signed.headers().asScala.foreach {
      case (name, values) =>
        if (!name.equalsIgnoreCase("Host")) {
          values.asScala.foreach(v => outbound = outbound.header(name, v))
        }
    }

    httpClient.send(outbound.build(), HttpResponse.BodyHandlers.ofInputStream())
  }

  private def writeResponse(
      upstream: HttpResponse[java.io.InputStream],
      resp: HttpServletResponse,
      streamBody: Boolean
  ): Unit = {
    resp.setStatus(upstream.statusCode())
    upstream.headers().map().asScala.foreach {
      case (name, values) =>
        val lower = name.toLowerCase
        if (forwardedResponseHeaderPrefixes.exists(lower.startsWith)) {
          values.asScala.foreach(v => resp.addHeader(name, v))
        }
    }

    if (streamBody) {
      val in = upstream.body()
      try {
        in.transferTo(resp.getOutputStream)
      } finally {
        in.close()
      }
    } else {
      upstream.body().close()
    }
  }
}
