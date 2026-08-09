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

package org.apache.texera.web.service

import com.fasterxml.jackson.databind.ObjectMapper
import kong.unirest.Unirest
import org.apache.texera.common.config.StorageConfig

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
  * Client for the Lakekeeper APIs used to manage per-user warehouses (#6870).
  *
  * Two API families are involved: the **management** API (`/management/v1/...`) creates and
  * deletes warehouse entities, and the **catalog** API (`/catalog/v1/{warehouseId}/...`) lists
  * and drops the namespaces/tables inside one. The channel is unauthenticated today;
  * catalog-side authentication is Phase 2 (#6040).
  *
  * @param catalogUri the Iceberg REST catalog uri (ends with `/catalog`), from which the
  *                   management base is derived. Overridable for tests.
  */
class LakekeeperClient(catalogUri: String = StorageConfig.icebergRESTCatalogUri) {

  // Lakekeeper's default project; single-project deployments (ours) use the nil UUID.
  private val DefaultProjectId = "00000000-0000-0000-0000-000000000000"

  private val managementBase: String = catalogUri.stripSuffix("/catalog") + "/management/v1"
  private val catalogBase: String = catalogUri + "/v1"

  private val mapper = new ObjectMapper()

  private def urlEncode(segment: String): String =
    URLEncoder.encode(segment, StandardCharsets.UTF_8)

  private def failOn(status: Int, body: String, action: String): Unit =
    if (status < 200 || status >= 300) {
      throw new RuntimeException(s"Lakekeeper $action failed (HTTP $status): $body")
    }

  /**
    * Creates a warehouse backed by this deployment's own object store (the Local flavor):
    * the storage profile points at the configured MinIO/S3 endpoint and bucket, with the
    * platform's static credentials and STS off.
    *
    * @return the Lakekeeper-assigned warehouse id.
    */
  def createWarehouse(warehouseName: String): UUID = {
    val payload = mapper.createObjectNode()
    payload.put("warehouse-name", warehouseName)
    payload.put("project-id", DefaultProjectId)

    val profile = payload.putObject("storage-profile")
    profile.put("type", "s3")
    profile.put("bucket", StorageConfig.icebergRESTCatalogS3Bucket)
    profile.put("region", StorageConfig.s3Region)
    profile.put("endpoint", StorageConfig.s3Endpoint)
    profile.put("path-style-access", true)
    // The warehouse name doubles as the key prefix, so each warehouse owns a distinct
    // subtree of the shared bucket.
    profile.put("key-prefix", warehouseName)
    profile.put("flavor", "s3-compat")
    profile.put("sts-enabled", false)

    val credential = payload.putObject("storage-credential")
    credential.put("type", "s3")
    credential.put("credential-type", "access-key")
    credential.put("aws-access-key-id", StorageConfig.s3Username)
    credential.put("aws-secret-access-key", StorageConfig.s3Password)

    val response = Unirest
      .post(s"$managementBase/warehouse")
      .header("Content-Type", "application/json")
      .body(payload.toString)
      .asString()
    failOn(response.getStatus, response.getBody, s"create warehouse '$warehouseName'")
    UUID.fromString(mapper.readTree(response.getBody).get("warehouse-id").asText())
  }

  /**
    * Deletes a warehouse **empty-first** (Lakekeeper refuses to drop a non-empty one):
    * every table is dropped with `purgeRequested=true` — so the underlying data files are
    * purged along with it, matching how execution results are deleted today — then the
    * namespaces, then the warehouse entity itself.
    */
  def deleteWarehouseEmptyFirst(warehouseId: UUID): Unit = {
    listNamespaces(warehouseId).foreach { namespace =>
      listTables(warehouseId, namespace).foreach { table =>
        val response = Unirest
          .delete(
            s"$catalogBase/$warehouseId/namespaces/${urlEncode(namespace)}/tables/${urlEncode(table)}"
          )
          .queryString("purgeRequested", "true")
          .asString()
        failOn(response.getStatus, response.getBody, s"drop table '$namespace.$table'")
      }
      val response = Unirest
        .delete(s"$catalogBase/$warehouseId/namespaces/${urlEncode(namespace)}")
        .asString()
      failOn(response.getStatus, response.getBody, s"drop namespace '$namespace'")
    }
    val response = Unirest.delete(s"$managementBase/warehouse/$warehouseId").asString()
    failOn(response.getStatus, response.getBody, "delete warehouse")
  }

  /** Top-level namespaces in the warehouse. Texera's execution namespaces are single-level. */
  private def listNamespaces(warehouseId: UUID): List[String] = {
    val response = Unirest.get(s"$catalogBase/$warehouseId/namespaces").asString()
    failOn(response.getStatus, response.getBody, "list namespaces")
    mapper
      .readTree(response.getBody)
      .get("namespaces")
      .iterator()
      .asScala
      .map(parts => parts.get(0).asText())
      .toList
  }

  private def listTables(warehouseId: UUID, namespace: String): List[String] = {
    val response = Unirest
      .get(s"$catalogBase/$warehouseId/namespaces/${urlEncode(namespace)}/tables")
      .asString()
    failOn(response.getStatus, response.getBody, s"list tables of '$namespace'")
    mapper
      .readTree(response.getBody)
      .get("identifiers")
      .iterator()
      .asScala
      .map(identifier => identifier.get("name").asText())
      .toList
  }
}
