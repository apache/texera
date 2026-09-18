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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.common.config.KubernetesConfig

import java.io.FileInputStream
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Paths}
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.time.Duration
import javax.net.ssl.{SSLContext, TrustManagerFactory}
import scala.jdk.CollectionConverters._

/**
  * Finds the node a computing unit's pod runs on, so a mount can be sent to that node's
  * mounter.
  *
  * Resolved here rather than supplied by the caller: letting a caller name the node would
  * hand anything that can reach this service the ability to aim requests at any node's
  * privileged mounter.
  */
class ComputingUnitNodeLocator(fetchPod: String => Option[JsonNode]) extends LazyLogging {

  def nodeIpOf(cuid: Int): Option[String] = {
    val podName = s"${KubernetesConfig.computeUnitPodNamePrefix}-$cuid"
    fetchPod(podName).map(_.at("/status/hostIP").asText("")).filter(_.nonEmpty)
  }
}

object ComputingUnitNodeLocator extends ComputingUnitNodeLocator(InClusterKubernetesApi.getPod)

// One read of one field, so the API is called directly rather than through a Kubernetes
// client library and its transitive dependencies.
private[util] object InClusterKubernetesApi extends LazyLogging {

  private val serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"
  private val mapper = new ObjectMapper()

  private lazy val client: HttpClient =
    HttpClient
      .newBuilder()
      .connectTimeout(Duration.ofSeconds(5))
      .sslContext(clusterSslContext)
      .build()

  /** Trusts only the cluster CA, so this talks to the API server and nothing else. */
  private def clusterSslContext: SSLContext = {
    val certificates = {
      val stream = new FileInputStream(s"$serviceAccountDir/ca.crt")
      try CertificateFactory.getInstance("X.509").generateCertificates(stream).asScala.toList
      finally stream.close()
    }
    val keyStore = KeyStore.getInstance(KeyStore.getDefaultType)
    keyStore.load(null, null)
    certificates.zipWithIndex.foreach {
      case (certificate, index) => keyStore.setCertificateEntry(s"cluster-ca-$index", certificate)
    }
    val trustManagerFactory =
      TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagerFactory.init(keyStore)
    val context = SSLContext.getInstance("TLS")
    context.init(null, trustManagerFactory.getTrustManagers, null)
    context
  }

  def getPod(podName: String): Option[JsonNode] = {
    val host = sys.env.getOrElse("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc")
    val port = sys.env.getOrElse("KUBERNETES_SERVICE_PORT", "443")
    val namespace = KubernetesConfig.computeUnitPoolNamespace
    val token = Files.readString(Paths.get(s"$serviceAccountDir/token")).trim

    val request = HttpRequest
      .newBuilder()
      .uri(URI.create(s"https://$host:$port/api/v1/namespaces/$namespace/pods/$podName"))
      .header("Authorization", s"Bearer $token")
      .timeout(Duration.ofSeconds(10))
      .GET()
      .build()

    val response = client.send(request, HttpResponse.BodyHandlers.ofString())
    response.statusCode() match {
      case 200   => Some(mapper.readTree(response.body()))
      case 404   => None
      case other =>
        // Distinguished from a missing pod: a missing RBAC rule or an unreachable API
        // server must not read as "the computing unit is not running".
        throw new IllegalStateException(
          s"cannot read pod $podName in namespace $namespace: HTTP $other ${response.body()}"
        )
    }
  }
}
