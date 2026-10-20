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
import org.apache.texera.auth.JwtParser
import org.apache.texera.common.config.StorageConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.SqlServer.withTransaction
import org.apache.texera.dao.jooq.generated.tables.daos.{DatasetDao, ModelDao}
import org.apache.texera.service.resource.{DatasetAccessResource, ModelAccessResource}
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.signer.AwsS3V4Signer
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams
import software.amazon.awssdk.http.{SdkHttpFullRequest, SdkHttpMethod}
import software.amazon.awssdk.regions.Region

import java.net.{URI, URLDecoder}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

/**
  * Read-only, JWT-authenticated, re-signing reverse proxy in front of the LakeFS S3
  * gateway. A computing-unit pod's GeeseFS mount talks to this servlet using the pod's
  * own per-user JWT as the S3 credential: the JWT is passed to GeeseFS as
  * `AWS_ACCESS_KEY_ID`, so it rides in the request's SigV4/SigV2 `Authorization` header.
  * Reusing the JWT that is already present in the pod means no separate mount credential
  * is ever issued, stored, or made multi-replica-consistent. The servlet:
  *
  *   1. reads the JWT back out of the incoming `Authorization` header (the JWT is the
  *      bearer capability; the pod-side S3 signature is not re-validated, and no LakeFS
  *      credentials ever leave this service),
  *   2. verifies the JWT and checks that its user has read access to the requested
  *      repository (the S3 bucket), using the same `userHasReadAccess` gate as the
  *      dataset REST endpoints, and
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

  private val forwardedResponseHeaderPrefixes =
    Seq("content-", "etag", "last-modified", "accept-ranges", "x-amz-")

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
    val user = S3ProxyServlet
      .extractCredentialToken(req.getHeader("Authorization"))
      .flatMap(token => JwtParser.parseToken(token).toScala)
    if (user.isEmpty) {
      // GeeseFS probes the bucket unauthenticated on mount, so this is expected noise.
      resp.sendError(HttpServletResponse.SC_FORBIDDEN, "missing or invalid user token")
      return
    }

    val uid = user.get.getUid
    val repositoryName = S3ProxyServlet.bucketFromUri(req.getRequestURI)
    if (repositoryName.isEmpty || !authorizedToRead(uid, repositoryName)) {
      logger.warn(
        s"user $uid denied mount access to repository '$repositoryName' for ${req.getRequestURI}"
      )
      resp.sendError(HttpServletResponse.SC_FORBIDDEN, "no read access to the requested repository")
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
    * True iff `uid` has read access to the versioned resource backing `repositoryName`.
    *
    * Read access to a repository grants read to all of its commits, so no per-commit check
    * is needed: a session addresses a single repository's data and any version the user may
    * already read.
    *
    * Both resource types are searched, because a repository backs either a dataset or a
    * model, and the name is matched rather than parsed: rows created today are named
    * `<type>-<id>`, but `sql/updates/15.sql` backfilled the column from the dataset's plain
    * `name`, so an upgraded deployment still has repositories called e.g. `my-data`.
    *
    * Exactly one row must match, or the request is denied. A repository name is unique in
    * practice -- LakeFS will not create two repositories with one name, and dataset creation
    * checked the name globally besides -- but `repository_name` carries no unique constraint
    * (the only UNIQUE on either table is (owner_uid, name)), so nothing in the schema says
    * so. Rather than let `.head` pick a row, and decide access arbitrarily in front of a
    * proxy that re-signs with the global LakeFS credentials, an ambiguous name fails closed.
    */
  private def authorizedToRead(uid: Integer, repositoryName: String): Boolean =
    withTransaction(SqlServer.getInstance().createDSLContext()) { ctx =>
      val datasets = new DatasetDao(ctx.configuration())
        .fetchByRepositoryName(repositoryName)
        .asScala
        .toList
      val models = new ModelDao(ctx.configuration())
        .fetchByRepositoryName(repositoryName)
        .asScala
        .toList
      (datasets, models) match {
        case (dataset :: Nil, Nil) =>
          DatasetAccessResource.userHasReadAccess(ctx, dataset.getDid, uid)
        case (Nil, model :: Nil) =>
          ModelAccessResource.userHasReadAccess(ctx, model.getMid, uid)
        case (Nil, Nil) =>
          false
        case _ =>
          // Logged because it means the data violates an assumption the mount path rests on,
          // and the owners of those rows will see an unexplained denial.
          logger.error(
            s"repository '$repositoryName' matches ${datasets.size} datasets and " +
              s"${models.size} models; denying rather than choosing one"
          )
          false
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

object S3ProxyServlet {

  /**
    * Extract the credential token from an AWS `Authorization` header. GeeseFS carries the
    * user JWT in the access-key-id position and signs with either SigV4
    * (`AWS4-HMAC-SHA256 Credential=<token>/<date>/...`) or, against a plain-HTTP custom
    * endpoint, SigV2 (`AWS <token>:<signature>`); support both. A JWT is base64url with
    * `.` separators, so it never contains the `/` or `:` these formats delimit on. The
    * pod-side S3 signature itself is not re-validated — the JWT is the bearer capability —
    * so only the token needs to be read out.
    */
  private[util] def extractCredentialToken(authHeader: String): Option[String] = {
    Option(authHeader).flatMap { h =>
      "Credential=([^/,\\s]+)/".r
        .findFirstMatchIn(h)
        .map(_.group(1)) // SigV4
        .orElse("^AWS ([^:\\s]+):".r.findFirstMatchIn(h.trim).map(_.group(1))) // SigV2
    }
  }

  /**
    * The repository (S3 bucket) a path-style request URI `/<bucket>/<key>` targets: its
    * first path segment, URL-decoded. Empty when the URI carries no bucket (root, or a
    * service-level list-buckets), which is never authorized.
    */
  private[util] def bucketFromUri(requestUri: String): String = {
    val firstSegment = requestUri.stripPrefix("/").split("/", 2)(0)
    if (firstSegment.isEmpty) "" else URLDecoder.decode(firstSegment, "UTF-8")
  }
}
