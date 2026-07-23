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

package org.apache.texera.amber.core.storage

import org.apache.texera.amber.util.IcebergUtil

import java.nio.file.Files

/**
  * Test-only helper that installs a local Hadoop-backed Iceberg catalog into the
  * shared `IcebergCatalogInstance` singleton exactly once per JVM.
  *
  * The default configured catalog type is `rest`, which needs a live REST server;
  * unit tests instead exercise Iceberg against a temp-directory `file:/` warehouse
  * (via the merged winutils-free local filesystem on Windows).
  *
  * `IcebergCatalogInstance` is a JVM-wide mutable singleton and ScalaTest runs
  * suites in the module in parallel within the same JVM, so every suite that needs
  * an Iceberg backend calls [[ensure]]. Initialization is idempotent and guarded by
  * a single flag on this shared object, so the very first caller wins and all
  * suites end up sharing the same catalog + warehouse instead of racing on
  * `replaceInstance`. Suites keep their tables disjoint via distinct namespaces and
  * unique (UUID) table names.
  */
object LocalHadoopIcebergCatalog {

  private var initialized = false

  def ensure(): Unit =
    synchronized {
      if (!initialized) {
        val warehouse = Files.createTempDirectory("wfcore-iceberg-shared")
        IcebergCatalogInstance.replaceInstance(
          IcebergUtil.createHadoopCatalog("wfcore-test", warehouse)
        )
        initialized = true
      }
    }
}
