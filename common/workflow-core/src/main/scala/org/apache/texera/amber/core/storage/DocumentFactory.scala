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

import org.apache.texera.common.config.StorageConfig
import org.apache.texera.amber.core.storage.FileResolver.DATASET_FILE_URI_SCHEME
import org.apache.texera.amber.core.storage.VFSResourceType._
import org.apache.texera.amber.core.storage.VFSURIFactory.{VFS_FILE_URI_SCHEME, decodeURI}
import org.apache.texera.amber.core.storage.model._
import org.apache.texera.amber.core.storage.result.iceberg.IcebergDocument
import org.apache.texera.amber.core.tuple.{Schema, Tuple}
import org.apache.texera.amber.util.IcebergUtil
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.Record
import org.apache.iceberg.{Schema => IcebergSchema}

import java.net.URI

object DocumentFactory {

  val ICEBERG = "iceberg"

  private def sanitizeURIPath(uri: URI): String =
    uri.getPath.stripPrefix("/").replace("/", "_")

  private def resolveNamespace(resourceType: VFSResourceType.Value): String =
    resourceType match {
      case RESULT             => StorageConfig.icebergTableResultNamespace
      case CONSOLE_MESSAGES   => StorageConfig.icebergTableConsoleMessagesNamespace
      case RUNTIME_STATISTICS => StorageConfig.icebergTableRuntimeStatisticsNamespace
      case STATE              => StorageConfig.icebergTableStateNamespace
      case _ =>
        throw new IllegalArgumentException(s"Resource type $resourceType is not supported")
    }

  /**
    * Open a document specified by the uri for read purposes only.
    * @param fileUri the uri of the document
    * @return ReadonlyVirtualDocument
    */
  def openReadonlyDocument(fileUri: URI): ReadonlyVirtualDocument[_] = {
    fileUri.getScheme match {
      case DATASET_FILE_URI_SCHEME => new DatasetFileDocument(fileUri)
      case "file"                  => new ReadonlyLocalFileDocument(fileUri)
      case unsupportedScheme =>
        throw new UnsupportedOperationException(
          s"Unsupported URI scheme: $unsupportedScheme for creating the ReadonlyDocument"
        )
    }
  }

  /**
    * Create a document for storage specified by the uri.
    * This document is suitable for storing structural data, i.e. the schema is required to create such document.
    * @param uri the location of the document
    * @param schema the schema of the data stored in the document
    * @return the created document
    */
  def createDocument(uri: URI, schema: Schema): VirtualDocument[_] = {
    uri.getScheme match {
      case VFS_FILE_URI_SCHEME =>
        val (_, _, _, resourceType) = decodeURI(uri)
        val storageKey = sanitizeURIPath(uri)
        val namespace = resolveNamespace(resourceType)

        val icebergSchema = IcebergUtil.toIcebergSchema(schema)
        IcebergUtil.createTable(
          IcebergCatalogInstance.getInstance(),
          namespace,
          storageKey,
          icebergSchema,
          overrideIfExists = true
        )
        val serde: (IcebergSchema, Tuple) => Record = IcebergUtil.toGenericRecord
        val deserde: (IcebergSchema, Record) => Tuple = (schema, record) =>
          IcebergUtil.fromRecord(record, IcebergUtil.fromIcebergSchema(schema))

        new IcebergDocument[Tuple](
          namespace,
          storageKey,
          icebergSchema,
          serde,
          deserde
        )
      case unsupportedScheme =>
        throw new UnsupportedOperationException(
          s"Unsupported URI scheme: $unsupportedScheme for creating the document"
        )
    }
  }

  /**
    * Check whether a document exists at the given URI without opening it.
    *
    * Returns true iff the underlying storage already has an entry for this
    * URI (e.g., an iceberg table at the resolved namespace + storage key).
    *
    * @throws UnsupportedOperationException if the URI scheme is not `vfs`.
    * @throws IllegalArgumentException if the resolved resource type has no
    *                                  iceberg namespace mapping.
    */
  def documentExists(uri: URI): Boolean = {
    uri.getScheme match {
      case VFS_FILE_URI_SCHEME =>
        val (_, _, _, resourceType) = decodeURI(uri)
        val storageKey = sanitizeURIPath(uri)
        val namespace = resolveNamespace(resourceType)
        IcebergCatalogInstance
          .getInstance()
          .tableExists(TableIdentifier.of(namespace, storageKey))

      case unsupportedScheme =>
        throw new UnsupportedOperationException(
          s"Unsupported URI scheme: $unsupportedScheme for checking document existence"
        )
    }
  }

  /**
    * Return the document at `uri`: when `reuseExisting` is set and a document
    * already exists there, open and return the existing one -- so a caller whose
    * output accumulates across re-runs (e.g. a LoopEnd port whose region
    * re-executes once per loop iteration) keeps the already-populated document
    * instead of clobbering it, since `createDocument` overrides any existing
    * document. Otherwise create it.
    *
    * `exists` / `open` / `create` default to this object's own `documentExists`
    * / `openDocument` / `createDocument`; they are parameterized only so the
    * create-or-reuse decision can be unit-tested without an iceberg backend.
    */
  def createOrReuseDocument(
      uri: URI,
      schema: Schema,
      reuseExisting: Boolean,
      exists: URI => Boolean = documentExists,
      open: URI => VirtualDocument[_] = (u: URI) => openDocument(u)._1,
      create: (URI, Schema) => VirtualDocument[_] = createDocument
  ): VirtualDocument[_] =
    if (reuseExisting && exists(uri)) open(uri) else create(uri, schema)

  /**
    * Open a document specified by the uri.
    * If the document is storing structural data, the schema will also be returned
    * @param uri the uri of the document
    * @return the VirtualDocument, which is the handler of the data; the Schema, which is the schema of the data stored in the document
    */
  def openDocument(uri: URI): (VirtualDocument[_], Option[Schema]) = {
    uri.getScheme match {
      case DATASET_FILE_URI_SCHEME => (new DatasetFileDocument(uri), None)
      case VFS_FILE_URI_SCHEME =>
        val (_, _, _, resourceType) = decodeURI(uri)
        val storageKey = sanitizeURIPath(uri)
        val namespace = resolveNamespace(resourceType)

        val table = IcebergUtil
          .loadTableMetadata(
            IcebergCatalogInstance.getInstance(),
            namespace,
            storageKey
          )
          .getOrElse(
            throw new IllegalArgumentException("No storage is found for the given URI")
          )

        val amberSchema = IcebergUtil.fromIcebergSchema(table.schema())
        val serde: (IcebergSchema, Tuple) => Record = IcebergUtil.toGenericRecord
        val deserde: (IcebergSchema, Record) => Tuple = (schema, record) =>
          IcebergUtil.fromRecord(record, IcebergUtil.fromIcebergSchema(schema))

        (
          new IcebergDocument[Tuple](
            namespace,
            storageKey,
            table.schema(),
            serde,
            deserde
          ),
          Some(amberSchema)
        )
      case unsupportedScheme =>
        throw new UnsupportedOperationException(
          s"Unsupported URI scheme: $unsupportedScheme for opening the document"
        )
    }
  }
}
