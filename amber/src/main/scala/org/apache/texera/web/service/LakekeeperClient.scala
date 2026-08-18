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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import kong.unirest.Unirest
import org.apache.texera.common.config.StorageConfig

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.mutable.ListBuffer
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
  * @param unfinishedTasksRetries how many times the final warehouse delete is retried while
  *                               Lakekeeper reports 409 WarehouseHasUnfinishedTasks — its
  *                               asynchronous purge of the dropped tables' data files is
  *                               still draining (#7742).
  * @param unfinishedTasksInitialDelayMillis first pause between those retries; it doubles up
  *                                          to the cap. Starting small keeps a fast purge
  *                                          (the common case) from costing the caller a full
  *                                          fixed interval, while the growth keeps a slow one
  *                                          from hammering Lakekeeper. Overridable for tests
  *                                          (0 keeps the spec free of real sleeps — doubling
  *                                          0 stays 0).
  * @param unfinishedTasksMaxDelayMillis cap for that doubling. With the defaults the retries
  *                                      wait 0.2+0.4+0.8+1.6+3.2+5+5s ≈ 16s in total.
  */
class LakekeeperClient(
    catalogUri: String = StorageConfig.icebergRESTCatalogUri,
    unfinishedTasksRetries: Int = 7,
    unfinishedTasksInitialDelayMillis: Long = 200,
    unfinishedTasksMaxDelayMillis: Long = 5000
) {

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
    // 404 anywhere below means the entity is already gone — the goal state. Tolerating
    // it makes this method idempotent, so a retry after a partial failure (e.g. the DB
    // delete failing after the Lakekeeper delete succeeded) heals instead of wedging.
    listNamespaces(warehouseId).foreach { namespace =>
      listTables(warehouseId, namespace).foreach { table =>
        val response = Unirest
          .delete(
            s"$catalogBase/$warehouseId/namespaces/${urlEncode(namespace)}/tables/${urlEncode(table)}"
          )
          .queryString("purgeRequested", "true")
          .asString()
        if (response.getStatus != 404) {
          failOn(response.getStatus, response.getBody, s"drop table '$namespace.$table'")
        }
      }
      val response = Unirest
        .delete(s"$catalogBase/$warehouseId/namespaces/${urlEncode(namespace)}")
        .asString()
      if (response.getStatus != 404) {
        failOn(response.getStatus, response.getBody, s"drop namespace '$namespace'")
      }
    }
    // The drops above purge each table's data files asynchronously (Lakekeeper task
    // queue `tabular_purge`), and Lakekeeper refuses to delete the warehouse while
    // any purge is pending — the tasks need the warehouse's storage profile to reach
    // S3, so deleting it first would orphan them and leak the files. It answers 409
    // WarehouseHasUnfinishedTasks until the queue drains (normally within seconds),
    // so ride that out with a bounded retry; every other error, including any other
    // 409, still fails immediately. (#7742)
    var attempt = 0
    var delay = unfinishedTasksInitialDelayMillis
    var deleted = false
    while (!deleted) {
      val response = Unirest.delete(s"$managementBase/warehouse/$warehouseId").asString()
      attempt += 1
      if (response.getStatus == 404 || (response.getStatus >= 200 && response.getStatus < 300)) {
        deleted = true
      } else if (
        isUnfinishedTasksConflict(response.getStatus, response.getBody) &&
        attempt <= unfinishedTasksRetries
      ) {
        Thread.sleep(delay)
        delay = math.min(delay * 2, unfinishedTasksMaxDelayMillis)
      } else {
        failOn(response.getStatus, response.getBody, "delete warehouse")
      }
    }
  }

  /** Lakekeeper's "purge queue still draining" conflict — the only retried error. */
  private def isUnfinishedTasksConflict(status: Int, body: String): Boolean =
    status == 409 && (try {
      mapper.readTree(body).path("error").path("type").asText() == "WarehouseHasUnfinishedTasks"
    } catch {
      case _: Exception => false
    })

  /** Top-level namespaces in the warehouse. Texera's execution namespaces are single-level. */
  private def listNamespaces(warehouseId: UUID): List[String] =
    fetchAllPages(s"$catalogBase/$warehouseId/namespaces", "namespaces", "list namespaces")(parts =>
      parts.get(0).asText()
    )

  private def listTables(warehouseId: UUID, namespace: String): List[String] =
    fetchAllPages(
      s"$catalogBase/$warehouseId/namespaces/${urlEncode(namespace)}/tables",
      "identifiers",
      s"list tables of '$namespace'"
    )(identifier => identifier.get("name").asText())

  /**
    * Follows `next-page-token` until exhausted: the Iceberg REST list endpoints may
    * return partial pages, and the empty-first delete depends on seeing everything.
    * A 404 ends the listing with what was gathered — the entity is already gone,
    * which the idempotent delete treats as its goal state.
    */
  private def fetchAllPages(url: String, field: String, action: String)(
      extract: JsonNode => String
  ): List[String] = {
    val results = ListBuffer[String]()
    var pageToken: Option[String] = None
    var more = true
    while (more) {
      val request = Unirest.get(url)
      pageToken.foreach(request.queryString("pageToken", _))
      val response = request.asString()
      if (response.getStatus == 404) {
        return results.toList
      }
      failOn(response.getStatus, response.getBody, action)
      val tree = mapper.readTree(response.getBody)
      tree.get(field).iterator().asScala.foreach(node => results += extract(node))
      pageToken = Option(tree.get("next-page-token"))
        .filterNot(_.isNull)
        .map(_.asText())
        .filter(_.nonEmpty)
      more = pageToken.isDefined
    }
    results.toList
  }
}
