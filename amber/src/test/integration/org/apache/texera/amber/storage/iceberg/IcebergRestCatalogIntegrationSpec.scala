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

package org.apache.texera.amber.storage.iceberg

import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.iceberg.rest.RESTCatalog
import org.apache.texera.amber.config.StorageConfig
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.tags.IntegrationTest
import org.apache.texera.amber.util.IcebergUtil
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID

/** Round-trip table metadata via the REST catalog. */
@IntegrationTest
class IcebergRestCatalogIntegrationSpec extends AnyFlatSpec with BeforeAndAfterAll {

  private var restCatalog: RESTCatalog = _

  private val testNamespace = "rest_integration_test"

  override def beforeAll(): Unit = {
    super.beforeAll()
    restCatalog = IcebergUtil.createRestCatalog(
      "rest_integration_test",
      StorageConfig.icebergRESTCatalogWarehouseName
    )
  }

  behavior of "Iceberg REST catalog"

  it should "round-trip table metadata via the REST catalog" in {
    val amberSchema = Schema(
      List(
        new Attribute("id", AttributeType.INTEGER),
        new Attribute("name", AttributeType.STRING)
      )
    )
    val icebergSchema = IcebergUtil.toIcebergSchema(amberSchema)

    val tableName = s"rest_table_${UUID.randomUUID().toString.replace("-", "")}"
    val identifier = TableIdentifier.of(testNamespace, tableName)

    IcebergUtil.createTable(
      restCatalog,
      testNamespace,
      tableName,
      icebergSchema,
      overrideIfExists = true
    )
    assert(restCatalog.tableExists(identifier))

    val loaded = restCatalog.loadTable(identifier)
    assert(loaded.schema().sameSchema(icebergSchema))

    restCatalog.dropTable(identifier, false)
    assert(!restCatalog.tableExists(identifier))
    intercept[NoSuchTableException] {
      restCatalog.loadTable(identifier)
    }
  }
}
